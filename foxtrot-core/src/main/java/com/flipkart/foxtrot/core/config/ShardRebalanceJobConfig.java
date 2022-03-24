package com.flipkart.foxtrot.core.config;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class ShardRebalanceJobConfig extends BaseJobConfig {

    private static final String JOB_NAME = "ShardRebalanceJob";

    @Getter
    @Setter
    private double shardCountThresholdPercentage = 20;

    @Override
    public String getJobName() {
        return JOB_NAME;
    }


}