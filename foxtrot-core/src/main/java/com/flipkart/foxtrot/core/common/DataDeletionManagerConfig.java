package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import lombok.NoArgsConstructor;

/**
 * Created by rishabh.goyal on 11/07/14.
 */
@NoArgsConstructor
public class DataDeletionManagerConfig extends BaseJobConfig {

    private static final String JOB_NAME = "DataDeletionJob";

    @Override
    public String getJobName() {
        return JOB_NAME;
    }
}
