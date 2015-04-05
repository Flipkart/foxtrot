package com.flipkart.foxtrot.core.querystore.actions.spi;

/**
 * Created by rishabh.goyal on 05/04/15.
 */
public enum AnalyticsOperation {

    group,
    count,
    trend,
    statstrend,
    distinct,
    query,
    histogram,
    stats,
    // For testing
    testCacheableFalse,


}
