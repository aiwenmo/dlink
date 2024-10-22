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

import org.dinky.job.JobManager;
import org.dinky.job.JobRunner;
import org.dinky.job.JobStatement;
import org.dinky.trans.ddl.CustomSetOperation;

public class JobSetRunner implements JobRunner {

    private JobManager jobManager;

    public JobSetRunner(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run(JobStatement jobStatement) throws Exception {
        CustomSetOperation customSetOperation = new CustomSetOperation(jobStatement.getStatement());
        customSetOperation.execute(jobManager.getExecutor().getCustomTableEnvironment());
    }
}
