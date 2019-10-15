package com.flipkart.foxtrot.common.stats;

import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
@NoArgsConstructor
public class StatsValue {

    private Map<String, Number> stats;
    private Map<Number, Number> percentiles;

    public Map<String, Number> getStats() {
        return stats;
    }

    public void setStats(Map<String, Number> stats) {
        this.stats = stats;
    }

    public Map<Number, Number> getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(Map<Number, Number> percentiles) {
        this.percentiles = percentiles;
    }
}
