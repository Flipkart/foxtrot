package com.flipkart.foxtrot.core.querystore.actions.spi;

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
public class AnalyticsConfig {

    private int precisionThreshold = 500;

    private int aggregationSize = 10000;
}
