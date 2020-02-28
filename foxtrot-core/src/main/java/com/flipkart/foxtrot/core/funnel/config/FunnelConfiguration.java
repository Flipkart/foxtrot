package com.flipkart.foxtrot.core.funnel.config;

import lombok.Data;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
public class FunnelConfiguration {

    private int expiryInDays = 30;
    private int querySize = 100;
    private String funnelConsoleUrl;
}
