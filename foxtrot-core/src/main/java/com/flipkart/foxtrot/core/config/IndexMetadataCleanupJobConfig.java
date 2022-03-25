package com.flipkart.foxtrot.core.config;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.Min;

@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IndexMetadataCleanupJobConfig extends BaseJobConfig {

    private static final String JOB_NAME = "TableIndexMetadataCleanupJob";

    @Getter
    @Setter
    @Min(30)
    private int retentionDays = 180;

    @Override
    public String getJobName() {
        return JOB_NAME;
    }


}