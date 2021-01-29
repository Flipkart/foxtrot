package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import lombok.Data;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
public class ClusterRerouteConfig extends BaseJobConfig {

    private static final String JOB_NAME = "ClusterReallocation";

    private double thresholdShardCountPercentage = 20;

    @Override
    public String getJobName() {
        return JOB_NAME;
    }
}
