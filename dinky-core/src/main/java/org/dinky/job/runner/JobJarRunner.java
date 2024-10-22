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
import org.dinky.classloader.DinkyClassLoader;
import org.dinky.data.exception.DinkyException;
import org.dinky.data.result.InsertResult;
import org.dinky.gateway.Gateway;
import org.dinky.gateway.config.GatewayConfig;
import org.dinky.gateway.result.GatewayResult;
import org.dinky.job.Job;
import org.dinky.job.JobManager;
import org.dinky.job.JobRunner;
import org.dinky.job.JobStatement;
import org.dinky.parser.SqlType;
import org.dinky.trans.Operations;
import org.dinky.trans.ddl.CustomSetOperation;
import org.dinky.trans.dml.ExecuteJarOperation;
import org.dinky.trans.parse.AddFileSqlParseStrategy;
import org.dinky.trans.parse.AddJarSqlParseStrategy;
import org.dinky.trans.parse.ExecuteJarParseStrategy;
import org.dinky.trans.parse.SetSqlParseStrategy;
import org.dinky.utils.DinkyClassLoaderUtil;
import org.dinky.utils.FlinkStreamEnvironmentUtil;
import org.dinky.utils.SqlUtil;
import org.dinky.utils.URLUtils;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cn.hutool.core.lang.Assert;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobJarRunner implements JobRunner {

    private JobManager jobManager;
    private final Configuration configuration;

    public JobJarRunner(JobManager jobManager) {
        this.jobManager = jobManager;
        configuration =
                jobManager.getExecutor().getCustomTableEnvironment().getConfig().getConfiguration();
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        if (!jobManager.isUseGateway()) {
            submitNormal();
        } else {
            GatewayResult gatewayResult;
            if (jobManager.getRunMode().isApplicationMode()) {
                gatewayResult = submitGateway();
            } else {
                gatewayResult = submitNormalWithGateway();
            }
            jobManager.getJob().setResult(InsertResult.success(gatewayResult.getId()));
            jobManager.getJob().setJobId(gatewayResult.getId());
            jobManager.getJob().setJids(gatewayResult.getJids());
            jobManager.getJob().setJobManagerAddress(URLUtils.formatAddress(gatewayResult.getWebURL()));

            if (gatewayResult.isSuccess()) {
                jobManager.getJob().setStatus(Job.JobStatus.SUCCESS);
            } else {
                jobManager.getJob().setStatus(Job.JobStatus.FAILED);
                jobManager.getJob().setError(gatewayResult.getError());
                log.error(gatewayResult.getError());
            }
        }
    }

    private GatewayResult submitGateway() throws Exception {
        configuration.set(PipelineOptions.JARS, getUris(jobManager.getJob().getStatement()));
        jobManager.getConfig().addGatewayConfig(configuration);
        jobManager.getConfig().getGatewayConfig().setSql(jobManager.getJob().getStatement());
        return Gateway.build(jobManager.getConfig().getGatewayConfig()).submitJar(jobManager.getUdfPathContextHolder());
    }

    private GatewayResult submitNormalWithGateway() {
        Pipeline pipeline = getPipeline();
        if (pipeline instanceof StreamGraph) {
            ((StreamGraph) pipeline).setJobName(jobManager.getConfig().getJobName());
        } else if (pipeline instanceof Plan) {
            ((Plan) pipeline).setJobName(jobManager.getConfig().getJobName());
        }
        JobGraph jobGraph = FlinkStreamEnvironmentUtil.getJobGraph(pipeline, configuration);
        GatewayConfig gatewayConfig = jobManager.getConfig().getGatewayConfig();
        List<String> uriList = getUris(jobManager.getJob().getStatement());
        String[] jarPaths = uriList.stream()
                .map(URLUtils::toFile)
                .map(File::getAbsolutePath)
                .toArray(String[]::new);
        gatewayConfig.setJarPaths(jarPaths);
        return Gateway.build(gatewayConfig).submitJobGraph(jobGraph);
    }

    private Pipeline getPipeline() {
        Pipeline pipeline = getJarStreamGraph(jobManager.getJob().getStatement(), jobManager.getDinkyClassLoader());
        if (pipeline instanceof StreamGraph) {
            if (Asserts.isNotNullString(jobManager.getConfig().getSavePointPath())) {
                ((StreamGraph) pipeline)
                        .setSavepointRestoreSettings(SavepointRestoreSettings.forPath(
                                jobManager.getConfig().getSavePointPath(),
                                configuration.get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)));
            }
        }
        return pipeline;
    }

    private void submitNormal() throws Exception {
        JobClient jobClient = FlinkStreamEnvironmentUtil.executeAsync(
                getPipeline(), jobManager.getExecutor().getStreamExecutionEnvironment());
        if (Asserts.isNotNull(jobClient)) {
            jobManager.getJob().setJobId(jobClient.getJobID().toHexString());
            jobManager.getJob().setJids(new ArrayList<String>() {
                {
                    add(jobManager.getJob().getJobId());
                }
            });
            jobManager.getJob().setStatus(Job.JobStatus.SUCCESS);
        } else {
            jobManager.getJob().setStatus(Job.JobStatus.FAILED);
        }
    }

    public Pipeline getJarStreamGraph(String statement, DinkyClassLoader dinkyClassLoader) {
        DinkyClassLoaderUtil.initClassLoader(jobManager.getConfig(), dinkyClassLoader);
        String[] statements = SqlUtil.getStatements(statement);
        ExecuteJarOperation executeJarOperation = null;
        for (String sql : statements) {
            String sqlStatement = jobManager.getExecutor().pretreatStatement(sql);
            if (ExecuteJarParseStrategy.INSTANCE.match(sqlStatement)) {
                executeJarOperation = new ExecuteJarOperation(sqlStatement);
                break;
            }
            SqlType operationType = Operations.getOperationType(sqlStatement);
            if (operationType.equals(SqlType.SET) && SetSqlParseStrategy.INSTANCE.match(sqlStatement)) {
                CustomSetOperation customSetOperation = new CustomSetOperation(sqlStatement);
                customSetOperation.execute(jobManager.getExecutor().getCustomTableEnvironment());
            } else if (operationType.equals(SqlType.ADD)) {
                Set<File> files = AddJarSqlParseStrategy.getAllFilePath(sqlStatement);
                files.forEach(jobManager.getExecutor()::addJar);
                files.forEach(jobManager.getUdfPathContextHolder()::addOtherPlugins);
            } else if (operationType.equals(SqlType.ADD_FILE)) {
                Set<File> files = AddFileSqlParseStrategy.getAllFilePath(sqlStatement);
                files.forEach(jobManager.getExecutor()::addJar);
                files.forEach(jobManager.getUdfPathContextHolder()::addFile);
            }
        }
        Assert.notNull(executeJarOperation, () -> new DinkyException("Not found execute jar operation."));
        List<URL> urLs = jobManager.getAllFileSet();
        return executeJarOperation.explain(jobManager.getExecutor().getCustomTableEnvironment(), urLs);
    }

    public List<String> getUris(String statement) {
        String[] statements = SqlUtil.getStatements(statement);
        List<String> uriList = new ArrayList<>();
        for (String sql : statements) {
            String sqlStatement = jobManager.getExecutor().pretreatStatement(sql);
            if (ExecuteJarParseStrategy.INSTANCE.match(sqlStatement)) {
                uriList.add(ExecuteJarParseStrategy.getInfo(statement).getUri());
                break;
            }
        }
        return uriList;
    }
}
