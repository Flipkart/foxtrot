package com.flipkart.foxtrot.core.funnel.util;

/***
 Created by mudit.g on Feb, 2019
 ***/
public class FunnelUtil {

    public static final String FILTERS = "filters";
    public static final String APPROVAL_REQUEST_SUBJECT = "Funnel approval needed";

    private FunnelUtil() {
    }

    public static String getApprovalRequestBody(String funnelName,
                                                String description,
                                                String endpoint) {
        return "Funnel approval needed. <br>Funnel Name: " + funnelName + " <br>Description: " + description
                + "<br>For details and to approve the funnel visit: " + endpoint;
    }
}
