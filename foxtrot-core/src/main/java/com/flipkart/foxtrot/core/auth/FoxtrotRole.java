package com.flipkart.foxtrot.core.auth;

import lombok.Getter;

/**
 *
 */
@Getter

public enum FoxtrotRole {
    INGEST(Value.INGEST),
    CONSOLE(Value.CONSOLE),
    QUERY(Value.QUERY),
    SYSADMIN(Value.SYSADMIN);

    private final String value;

    private FoxtrotRole(String value) {
        this.value = value;
    }

    public static class Value {

        public static final String INGEST = "INGEST";
        public static final String CONSOLE = "CONSOLE";
        public static final String QUERY = "QUERY";
        public static final String SYSADMIN = "SYSADMIN";
    }
}
