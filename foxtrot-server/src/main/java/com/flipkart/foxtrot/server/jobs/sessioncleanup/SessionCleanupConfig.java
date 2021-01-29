package com.flipkart.foxtrot.server.jobs.sessioncleanup;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;

/**
 *
 */
public class SessionCleanupConfig extends BaseJobConfig {
    private static final String JOB_NAME = "ExpiredSessionCleaner";

    @Override
    public String getJobName() {
        return JOB_NAME;
    }
}
