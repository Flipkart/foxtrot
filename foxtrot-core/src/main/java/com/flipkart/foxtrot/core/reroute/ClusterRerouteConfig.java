package com.flipkart.foxtrot.core.reroute;

import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
public class ClusterRerouteConfig {
    private static final String JOB_NAME = "ClusterReroute";

    private boolean rerouteEnabled = true;

    private int waitBeforeNextReallocationInMinutes = 20;

    private int cacheSize = 20;

    private String recipients = "mudit.g@phonepe.com, nitish.goyal@phonepe.com";

    private int noOfRetries = 3;

    private int noOfShardsToBeReallocated = 2;

    private List<String> exceptionMessages = new ArrayList<>(Arrays.asList("EsRejectedExecutionException"));

    private int maxTimeToRunRerouteJobInMinutes = 5;

    public static String getJobName() {
        return JOB_NAME;
    }
}
