package com.flipkart.foxtrot.core.funnel.config;

import lombok.Data;

@Data
public class BaseEventConfig {
    private String eventType = "APP_LOADED";
    private String category = "APP_LOADED";
}
