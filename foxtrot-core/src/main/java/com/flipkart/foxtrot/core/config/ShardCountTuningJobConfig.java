package com.flipkart.foxtrot.core.config;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.Min;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ShardCountTuningJobConfig extends BaseJobConfig {

    private static final String JOB_NAME = "ShardCountTuningJob";


    @Min(20)
    private double idealShardSizeInGBs = 20;


    @Min(7)
    private int rollingWindowInDays = 7;

    @Min(1)
    private int replicationFactor = 2;

    private boolean shardCountTuningEnabled = false;

    @Override
    public String getJobName() {
        return JOB_NAME;
    }


}