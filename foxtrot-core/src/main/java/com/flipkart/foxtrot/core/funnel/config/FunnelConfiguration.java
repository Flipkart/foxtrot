package com.flipkart.foxtrot.core.funnel.config;

import lombok.Builder;
import lombok.Data;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@Builder
public class FunnelConfiguration {

    private int querySize = 100;

    private String funnelIndex = "foxtrot_funnel";

    private FunnelDropdownConfig funnelDropdownConfig;

    private BaseFunnelEventConfig baseFunnelEventConfig;

    private String funnelConsoleUrl;
}
