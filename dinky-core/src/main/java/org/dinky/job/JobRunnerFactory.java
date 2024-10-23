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

package org.dinky.job;

import org.dinky.job.runner.JobAddRunner;
import org.dinky.job.runner.JobDDLRunner;
import org.dinky.job.runner.JobExecuteRunner;
import org.dinky.job.runner.JobPrintRunner;
import org.dinky.job.runner.JobSetRunner;
import org.dinky.job.runner.JobSqlRunner;
import org.dinky.job.runner.JobUDFRunner;

public class JobRunnerFactory {

    private JobSetRunner jobSetRunner;
    private JobAddRunner jobAddRunner;
    private JobSqlRunner jobSqlRunner;
    private JobExecuteRunner jobExecuteRunner;
    private JobUDFRunner jobUDFRunner;
    private JobPrintRunner jobPrintRunner;
    private JobDDLRunner jobDDLRunner;

    public JobRunnerFactory(JobManager jobManager) {
        this.jobSetRunner = new JobSetRunner(jobManager);
        this.jobAddRunner = new JobAddRunner(jobManager);
        this.jobSqlRunner = new JobSqlRunner(jobManager);
        this.jobExecuteRunner = new JobExecuteRunner(jobManager);
        this.jobUDFRunner = new JobUDFRunner(jobManager);
        this.jobPrintRunner = new JobPrintRunner(jobManager);
        this.jobDDLRunner = new JobDDLRunner(jobManager);
    }

    public JobRunner getJobRunner(JobStatementType jobStatementType) {
        switch (jobStatementType) {
            case SET:
                return jobSetRunner;
            case ADD:
            case ADD_FILE:
            case ADD_JAR:
                return jobAddRunner;
            case SQL:
                return jobSqlRunner;
            case EXECUTE:
                return jobExecuteRunner;
            case UDF:
                return jobUDFRunner;
            case PRINT:
                return jobPrintRunner;
            case DDL:
            default:
                return jobDDLRunner;
        }
    }

    public static JobRunnerFactory create(JobManager jobManager) {
        return new JobRunnerFactory(jobManager);
    }
}
