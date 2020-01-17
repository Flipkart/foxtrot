package com.flipkart.foxtrot.core.config;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;

/***
 Created by mudit.g on Dec, 2018
 ***/
public class ConsoleHistoryConfig extends BaseJobConfig {

    private static final String JOB_NAME = "ConsoleHistory";

    @Override
    public String getJobName() {
        return JOB_NAME;
    }
}
