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

import org.dinky.parser.SqlType;

import java.util.ArrayList;
import java.util.List;

public class JobStatementPlan {

    private List<JobStatement> jobStatementList = new ArrayList<>();

    public JobStatementPlan() {}

    public List<JobStatement> getJobStatementList() {
        return jobStatementList;
    }

    public void addJobStatement(String statement, JobStatementType statementType, SqlType sqlType) {
        jobStatementList.add(new JobStatement(jobStatementList.size() + 1, statement, statementType, sqlType, false));
    }

    public void addJobStatementGenerated(String statement, JobStatementType statementType, SqlType sqlType) {
        jobStatementList.add(new JobStatement(jobStatementList.size() + 1, statement, statementType, sqlType, true));
    }

    public void buildFinalExecutableStatement() {
        if (jobStatementList.size() == 0) {
            return;
        }
        int index = -1;
        for (int i = 0; i < jobStatementList.size(); i++) {
            if (JobStatementType.SQL.equals(jobStatementList.get(i).getStatementType())) {
                index = i;
            }
        }
        if (index < 0) {
            return;
        }
        jobStatementList.get(index).asFinalExecutableStatement();
    }
}
