package com.flipkart.foxtrot.core.reroute;

import lombok.Data;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
public class ClusterRerouteConfig {

    private double thresholdShardCountPercentage = 20;
}
