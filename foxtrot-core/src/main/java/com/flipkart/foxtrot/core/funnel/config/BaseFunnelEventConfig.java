package com.flipkart.foxtrot.core.funnel.config;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BaseFunnelEventConfig {

    private String eventType = "APP_LOADED";
    private String category = "APP_LOADED";
}
