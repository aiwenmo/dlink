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

import org.dinky.executor.CustomTableEnvironment;
import org.dinky.job.AbstractJobRunner;
import org.dinky.job.JobManager;
import org.dinky.job.JobStatement;
import org.dinky.trans.parse.AddFileSqlParseStrategy;
import org.dinky.trans.parse.AddJarSqlParseStrategy;
import org.dinky.utils.URLUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobAddRunner extends AbstractJobRunner {

    public JobAddRunner(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        switch (jobStatement.getStatementType()) {
            case ADD:
                AddJarSqlParseStrategy.getAllFilePath(jobStatement.getStatement())
                        .forEach(t -> jobManager.getUdfPathContextHolder().addOtherPlugins(t));
                (jobManager.getExecutor().getDinkyClassLoader())
                        .addURLs(URLUtils.getURLs(
                                jobManager.getUdfPathContextHolder().getOtherPluginsFiles()));
                break;
            case ADD_FILE:
                AddFileSqlParseStrategy.getAllFilePath(jobStatement.getStatement())
                        .forEach(t -> jobManager.getUdfPathContextHolder().addFile(t));
                (jobManager.getExecutor().getDinkyClassLoader())
                        .addURLs(URLUtils.getURLs(
                                jobManager.getUdfPathContextHolder().getFiles()));
                break;
            case ADD_JAR:
                Configuration combinationConfig = getCombinationConfig();
                FileSystem.initialize(combinationConfig, null);
                jobManager.getExecutor().executeSql(jobStatement.getStatement());
                break;
        }
    }

    private Configuration getCombinationConfig() {
        CustomTableEnvironment cte = jobManager.getExecutor().getCustomTableEnvironment();
        Configuration rootConfig = cte.getRootConfiguration();
        Configuration config = cte.getConfig().getConfiguration();
        Configuration combinationConfig = new Configuration();
        combinationConfig.addAll(rootConfig);
        combinationConfig.addAll(config);
        return combinationConfig;
    }
}
