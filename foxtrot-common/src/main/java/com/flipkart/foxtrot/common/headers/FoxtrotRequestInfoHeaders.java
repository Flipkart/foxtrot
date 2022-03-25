package com.flipkart.foxtrot.common.headers;

public final class FoxtrotRequestInfoHeaders {

    public static final String SOURCE_TYPE = "X-SOURCE-TYPE";
    public static final String SERVICE_NAME = "X-SERVICE-NAME";
    public static final String SCRIPT_NAME = "X-SCRIPT_NAME";

    private FoxtrotRequestInfoHeaders() {
        throw new IllegalStateException("Utility class");
    }

}