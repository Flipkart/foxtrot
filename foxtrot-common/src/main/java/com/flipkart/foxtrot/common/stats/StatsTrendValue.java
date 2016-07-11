package com.flipkart.foxtrot.common.stats;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
public class StatsTrendValue extends StatsValue {

    private Number period;

    public StatsTrendValue() {

    }

    public Number getPeriod() {
        return period;
    }

    public void setPeriod(Number period) {
        this.period = period;
    }
}
