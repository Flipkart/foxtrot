package com.flipkart.foxtrot.core.funnel.config;

import lombok.Data;

@Data
public class BaseEventConfig {
    private String eventId = "APP_LOADED";
    private String identifierId = "APP_LOADED";
}
