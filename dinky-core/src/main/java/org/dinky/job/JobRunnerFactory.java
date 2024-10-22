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
    public static JobRunner getJobRunner(JobStatementType jobStatementType, JobManager jobManager) {
        switch (jobStatementType) {
            case SET:
                return new JobSetRunner(jobManager);
            case ADD:
            case ADD_FILE:
            case ADD_JAR:
                return new JobAddRunner(jobManager);
            case SQL:
                return new JobSqlRunner(jobManager);
            case EXECUTE:
                return new JobExecuteRunner(jobManager);
            case UDF:
                return new JobUDFRunner(jobManager);
            case PRINT:
                return new JobPrintRunner(jobManager);
            case DDL:
            default:
                return new JobDDLRunner(jobManager);
        }
    }
}
