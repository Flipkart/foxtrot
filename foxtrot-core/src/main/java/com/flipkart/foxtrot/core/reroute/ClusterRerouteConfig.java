package com.flipkart.foxtrot.core.reroute;

import lombok.Data;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
public class ClusterRerouteConfig {

    private int waitBeforeNextReallocation = 60*20;

    private int cacheSize = 20;

    private String recipients = "mudit.g@phonepe.com, nitish.goyal@phonepe.com";

    private int noOfRetries = 3;

    private int noOfShardsToBeRealocated = 2;
}
