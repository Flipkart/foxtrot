package com.flipkart.foxtrot.common.stats;

import java.util.Collection;

/**
 *
 */
public enum AnalyticsRequestFlags {
    STATS_SKIP_PERCENTILES;

    public static boolean hasFlag(Collection<AnalyticsRequestFlags> flags, AnalyticsRequestFlags requestFlag) {
        return null != flags && flags.contains(requestFlag);
    }
}
