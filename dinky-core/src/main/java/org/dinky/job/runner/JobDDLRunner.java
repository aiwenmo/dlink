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
import org.dinky.data.result.SqlExplainResult;
import org.dinky.job.AbstractJobRunner;
import org.dinky.job.JobManager;
import org.dinky.job.JobStatement;
import org.dinky.utils.LogUtil;
import org.dinky.utils.SqlUtil;

import java.time.LocalDateTime;

import cn.hutool.core.text.StrFormatter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobDDLRunner extends AbstractJobRunner {

    public JobDDLRunner(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        jobManager.getExecutor().executeSql(jobStatement.getStatement());
    }

    @Override
    public SqlExplainResult explain(JobStatement jobStatement) {
        SqlExplainResult.Builder resultBuilder = SqlExplainResult.Builder.newBuilder();
        try {
            SqlExplainResult recordResult = jobManager.getExecutor().explainSqlRecord(jobStatement.getStatement());
            if (Asserts.isNull(recordResult)) {
                return resultBuilder.isSkipped().build();
            }
            resultBuilder = SqlExplainResult.newBuilder(recordResult);
            // Flink DDL needs to execute to create catalog.
            run(jobStatement);
            resultBuilder
                    .explainTrue(true)
                    .type(jobStatement.getSqlType().getType())
                    .sql(jobStatement.getStatement())
                    .index(jobStatement.getIndex());
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
}
