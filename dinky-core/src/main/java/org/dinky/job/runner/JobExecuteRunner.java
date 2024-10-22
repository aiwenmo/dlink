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
import org.dinky.data.result.IResult;
import org.dinky.data.result.InsertResult;
import org.dinky.data.result.ResultBuilder;
import org.dinky.gateway.Gateway;
import org.dinky.gateway.result.GatewayResult;
import org.dinky.job.Job;
import org.dinky.job.JobManager;
import org.dinky.job.JobRunner;
import org.dinky.job.JobStatement;
import org.dinky.parser.SqlType;
import org.dinky.utils.URLUtils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.ArrayList;
import java.util.List;

public class JobExecuteRunner implements JobRunner {

    private JobManager jobManager;
    private List<String> statements;

    public JobExecuteRunner(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        statements.add(jobStatement.getStatement());
        jobManager.getExecutor().executeSql(jobStatement.getStatement());
        if (jobStatement.isFinalExecutableStatement()) {
            if (jobManager.isUseGateway()) {
                processWithGateway();
            } else {
                processWithoutGateway();
            }
        }
    }

    private void processWithGateway() throws Exception {
        GatewayResult gatewayResult = null;
        jobManager.getConfig().addGatewayConfig(jobManager.getExecutor().getSetConfig());

        if (jobManager.getRunMode().isApplicationMode()) {
            jobManager.getConfig().getGatewayConfig().setSql(getParsedSql());
            gatewayResult = Gateway.build(jobManager.getConfig().getGatewayConfig())
                    .submitJar(jobManager.getExecutor().getDinkyClassLoader().getUdfPathContextHolder());
        } else {
            StreamGraph streamGraph = jobManager.getExecutor().getStreamGraph();
            streamGraph.setJobName(jobManager.getConfig().getJobName());
            JobGraph jobGraph = streamGraph.getJobGraph();
            if (Asserts.isNotNullString(jobManager.getConfig().getSavePointPath())) {
                jobGraph.setSavepointRestoreSettings(
                        SavepointRestoreSettings.forPath(jobManager.getConfig().getSavePointPath(), true));
            }
            gatewayResult =
                    Gateway.build(jobManager.getConfig().getGatewayConfig()).submitJobGraph(jobGraph);
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
        }
    }

    private void processWithoutGateway() throws Exception {
        JobClient jobClient =
                jobManager.getExecutor().executeAsync(jobManager.getConfig().getJobName());
        if (Asserts.isNotNull(jobClient)) {
            jobManager.getJob().setJobId(jobClient.getJobID().toHexString());
            jobManager.getJob().setJids(new ArrayList<String>() {
                {
                    add(jobManager.getJob().getJobId());
                }
            });
        }
        if (jobManager.getConfig().isUseResult()) {
            IResult result = ResultBuilder.build(
                            SqlType.EXECUTE,
                            jobManager.getJob().getId().toString(),
                            jobManager.getConfig().getMaxRowNum(),
                            jobManager.getConfig().isUseChangeLog(),
                            jobManager.getConfig().isUseAutoCancel(),
                            jobManager.getExecutor().getTimeZone())
                    .getResult(null);
            jobManager.getJob().setResult(result);
        }
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
