package com.flipkart.foxtrot.core.funnel.config;

import com.flipkart.foxtrot.common.query.Filter;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.NotNull;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@Builder
public class FunnelConfiguration {

    private int querySize = 100;

    private String funnelIndex = "foxtrot_funnel";

    private FunnelDropdownConfig funnelDropdownConfig;

    @NotNull
    private BaseFunnelEventConfig baseFunnelEventConfig;

    @NotNull
    private Filter defaultVersionCodeFilter;

    private String funnelConsoleUrl;

    private boolean enabled;
}
