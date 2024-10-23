/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.job.runner;

import org.dinky.assertion.Asserts;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.result.IResult;
import org.dinky.data.result.InsertResult;
import org.dinky.data.result.ResultBuilder;
import org.dinky.data.result.SqlExplainResult;
import org.dinky.executor.Executor;
import org.dinky.gateway.Gateway;
import org.dinky.gateway.result.GatewayResult;
import org.dinky.interceptor.FlinkInterceptor;
import org.dinky.interceptor.FlinkInterceptorResult;
import org.dinky.job.AbstractJobRunner;
import org.dinky.job.Job;
import org.dinky.job.JobConfig;
import org.dinky.job.JobManager;
import org.dinky.job.JobStatement;
import org.dinky.parser.SqlType;
import org.dinky.utils.LogUtil;
import org.dinky.utils.SqlUtil;
import org.dinky.utils.URLUtils;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import cn.hutool.core.text.StrFormatter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobSqlRunner extends AbstractJobRunner {

    private List<ModifyOperation> modifyOperations;
    private List<String> statements;
    private List<Operation> operations;

    public JobSqlRunner(JobManager jobManager) {
        this.jobManager = jobManager;
        this.modifyOperations = new ArrayList<>();
        this.statements = new ArrayList<>();
        this.operations = new ArrayList<>();
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        if (jobManager.isUseStatementSet()) {
            handleStatementSet(jobStatement);
        } else {
            handleNonStatementSet(jobStatement);
        }
    }

    @Override
    public SqlExplainResult explain(JobStatement jobStatement) {
        SqlExplainResult.Builder resultBuilder = SqlExplainResult.Builder.newBuilder();

        try {
            ModifyOperation modifyOperation =
                    jobManager.getExecutor().getModifyOperationFromInsert(jobStatement.getStatement());
            operations.add(modifyOperation);
            statements.add(jobStatement.getStatement());
            if (jobStatement.isFinalExecutableStatement()) {
                SqlExplainResult sqlExplainResult = jobManager.getExecutor().explainOperation(operations);
                resultBuilder = SqlExplainResult.newBuilder(sqlExplainResult);
                resultBuilder.sql(getParsedSql()).index(jobStatement.getIndex());
            } else {
                resultBuilder
                        .sql(getParsedSql())
                        .type(jobStatement.getSqlType().getType())
                        .parseTrue(true)
                        .explainTrue(true)
                        .index(jobStatement.getIndex())
                        .isSkipped();
            }
        } catch (Exception e) {
            String error = StrFormatter.format(
                    "Exception in explaining FlinkSQL:\n{}\n{}",
                    SqlUtil.addLineNumber(jobStatement.getStatement()),
                    LogUtil.getError(e));
            resultBuilder
                    .error(error)
                    .explainTrue(false)
                    .type(jobStatement.getSqlType().getType())
                    .sql(jobStatement.getStatement())
                    .index(jobStatement.getIndex());
            log.error(error);
        } finally {
            resultBuilder.explainTime(LocalDateTime.now());
            return resultBuilder.build();
        }
    }

    private void handleStatementSet(JobStatement jobStatement) throws Exception {
        if (jobManager.isUseGateway()) {
            processWithGateway(jobStatement);
        } else {
            processWithoutGateway(jobStatement);
        }
    }

    private void handleNonStatementSet(JobStatement jobStatement) throws Exception {
        if (jobManager.isUseGateway()) {
            processWithGateway(jobStatement);
        } else {
            processFirstStatement(jobStatement);
        }
    }

    private void processWithGateway(JobStatement jobStatement) throws Exception {
        GatewayResult gatewayResult = submitByGateway(jobStatement);
        if (Asserts.isNotNull(gatewayResult)) {
            setJobResultFromGatewayResult(gatewayResult);
        }
    }

    private void processWithoutGateway(JobStatement jobStatement) throws Exception {
        ModifyOperation modifyOperation =
                jobManager.getExecutor().getModifyOperationFromInsert(jobStatement.getStatement());
        modifyOperations.add(modifyOperation);
        statements.add(jobStatement.getStatement());
        if (jobStatement.isFinalExecutableStatement()) {
            TableResult tableResult = jobManager.getExecutor().executeModifyOperations(modifyOperations);
            updateJobWithTableResult(tableResult);
        }
    }

    private void processFirstStatement(JobStatement jobStatement) throws Exception {
        processSingleStatement(jobStatement);
    }

    private void processSingleStatement(JobStatement jobStatement) throws Exception {
        FlinkInterceptorResult flinkInterceptorResult =
                FlinkInterceptor.build(jobManager.getExecutor(), jobStatement.getStatement());
        if (Asserts.isNotNull(flinkInterceptorResult.getTableResult())) {
            updateJobWithTableResult(flinkInterceptorResult.getTableResult(), jobStatement.getSqlType());
        } else if (!flinkInterceptorResult.isNoExecute()) {
            TableResult tableResult = jobManager.getExecutor().executeSql(jobStatement.getStatement());
            updateJobWithTableResult(tableResult, jobStatement.getSqlType());
        }
    }

    private void setJobResultFromGatewayResult(GatewayResult gatewayResult) {
        jobManager.getJob().setResult(InsertResult.success(gatewayResult.getId()));
        jobManager.getJob().setJobId(gatewayResult.getId());
        jobManager.getJob().setJids(gatewayResult.getJids());
        jobManager.getJob().setJobManagerAddress(URLUtils.formatAddress(gatewayResult.getWebURL()));
        jobManager.getJob().setStatus(gatewayResult.isSuccess() ? Job.JobStatus.SUCCESS : Job.JobStatus.FAILED);
        if (!gatewayResult.isSuccess()) {
            jobManager.getJob().setError(gatewayResult.getError());
        }
    }

    private void updateJobWithTableResult(TableResult tableResult) {
        updateJobWithTableResult(tableResult, SqlType.INSERT);
    }

    private void updateJobWithTableResult(TableResult tableResult, SqlType sqlType) {
        if (tableResult.getJobClient().isPresent()) {
            jobManager
                    .getJob()
                    .setJobId(tableResult.getJobClient().get().getJobID().toHexString());
            jobManager
                    .getJob()
                    .setJids(Collections.singletonList(jobManager.getJob().getJobId()));
        } else if (!sqlType.getCategory().getHasJobClient()) {
            jobManager.getJob().setJobId(UUID.randomUUID().toString().replace("-", ""));
            jobManager
                    .getJob()
                    .setJids(Collections.singletonList(jobManager.getJob().getJobId()));
        }

        if (jobManager.getConfig().isUseResult()) {
            IResult result = ResultBuilder.build(
                            sqlType,
                            jobManager.getJob().getId().toString(),
                            jobManager.getConfig().getMaxRowNum(),
                            jobManager.getConfig().isUseChangeLog(),
                            jobManager.getConfig().isUseAutoCancel(),
                            jobManager.getExecutor().getTimeZone())
                    .getResultWithPersistence(tableResult, jobManager.getHandler());
            jobManager.getJob().setResult(result);
        }
    }

    private GatewayResult submitByGateway(JobStatement jobStatement) {
        GatewayResult gatewayResult = null;

        JobConfig config = jobManager.getConfig();
        GatewayType runMode = jobManager.getRunMode();
        Executor executor = jobManager.getExecutor();

        statements.add(jobStatement.getStatement());
        // Use gateway need to build gateway config, include flink configuration.
        config.addGatewayConfig(executor.getCustomTableEnvironment().getConfig().getConfiguration());

        if (runMode.isApplicationMode()) {
            if (!jobStatement.isFinalExecutableStatement()) {
                return gatewayResult;
            }
            // Application mode need to submit dinky-app.jar that in the hdfs or image.
            config.getGatewayConfig().setSql(getParsedSql());
            gatewayResult = Gateway.build(config.getGatewayConfig())
                    .submitJar(executor.getDinkyClassLoader().getUdfPathContextHolder());
        } else {
            ModifyOperation modifyOperation = executor.getModifyOperationFromInsert(jobStatement.getStatement());
            modifyOperations.add(modifyOperation);
            if (!jobStatement.isFinalExecutableStatement()) {
                return gatewayResult;
            }
            JobGraph jobGraph = executor.getStreamGraphFromModifyOperations(modifyOperations)
                    .getJobGraph();
            // Perjob mode need to set savepoint restore path, when recovery from savepoint.
            if (Asserts.isNotNullString(config.getSavePointPath())) {
                jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(config.getSavePointPath(), true));
            }
            // Perjob mode need to submit job graph.
            gatewayResult = Gateway.build(config.getGatewayConfig()).submitJobGraph(jobGraph);
        }
        return gatewayResult;
    }

    private String getParsedSql() {
        StringBuilder sb = new StringBuilder();
        for (String statement : statements) {
            if (sb.length() > 0) {
                sb.append(";\n");
            }
            sb.append(statement);
        }
        return sb.toString();
    }
}
