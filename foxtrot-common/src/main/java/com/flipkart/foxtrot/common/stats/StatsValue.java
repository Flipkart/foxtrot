package com.flipkart.foxtrot.common.stats;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

import lombok.NoArgsConstructor;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
@NoArgsConstructor
public class StatsValue {

    private Map<String, Number> stats;

    @JsonDeserialize(keyUsing = StatsValueDeserializer.class)
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
