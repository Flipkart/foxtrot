package com.flipkart.foxtrot.core.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticsearchTuningConfig {

    private int precisionThreshold = 500;

    private int aggregationSize = 10000;

    private int scrollTimeInSeconds = 120;

    private int documentsLimitAllowed = 10000;
}
