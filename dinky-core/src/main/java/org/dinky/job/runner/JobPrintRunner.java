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

import org.dinky.explainer.print_table.PrintStatementExplainer;
import org.dinky.job.JobManager;
import org.dinky.job.JobRunner;
import org.dinky.job.JobStatement;
import org.dinky.job.JobStatementType;
import org.dinky.parser.SqlType;
import org.dinky.utils.IpUtil;

import java.util.Map;

public class JobPrintRunner implements JobRunner {

    private JobManager jobManager;

    public JobPrintRunner(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        Map<String, String> config =
                jobManager.getExecutor().getExecutorConfig().getConfig();
        String host = config.getOrDefault("dinky.dinkyHost", IpUtil.getHostIp());
        int port = Integer.parseInt(config.getOrDefault("dinky.dinkyPrintPort", "7125"));
        String[] tableNames = PrintStatementExplainer.getTableNames(jobStatement.getStatement());
        for (String tableName : tableNames) {
            String ctasStatement = PrintStatementExplainer.getCreateStatement(tableName, host, port);
            JobStatement ctasJobStatement = JobStatement.generateJobStatement(
                    jobStatement.getIndex(), ctasStatement, JobStatementType.SQL, SqlType.CTAS);
            JobSqlRunner jobSqlRunner = new JobSqlRunner(jobManager);
            jobSqlRunner.run(ctasJobStatement);
        }
    }
}
