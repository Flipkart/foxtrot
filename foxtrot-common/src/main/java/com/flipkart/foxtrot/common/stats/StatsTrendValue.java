package com.flipkart.foxtrot.common.stats;

import lombok.NoArgsConstructor;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
@NoArgsConstructor
public class StatsTrendValue extends StatsValue {

    private Number period;

    public Number getPeriod() {
        return period;
    }

    public void setPeriod(Number period) {
        this.period = period;
    }
}
