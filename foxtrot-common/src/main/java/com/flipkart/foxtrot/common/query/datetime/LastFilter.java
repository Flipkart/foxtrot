package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import io.dropwizard.util.Duration;


import javax.validation.constraints.NotNull;

public class LastFilter extends Filter {

    private long currentTime;

    @NotNull
    private Duration duration;

    public LastFilter() {
        super(FilterOperator.last);
        currentTime = System.currentTimeMillis();
        super.setField("_timestamp");
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    @Override
    public boolean equals(Object rhs) {
        if (this == rhs) return true;
        if (rhs == null || getClass() != rhs.getClass()) return false;
        if (!super.equals(rhs)) return false;

        LastFilter that = (LastFilter) rhs;
        return getWindow().equals(that.getWindow());
    }

    @Override
    public int hashCode() {
        TimeWindow timeWindow = getWindow();
        int result = 0;
        result = 31 * result + Long.valueOf(timeWindow.getStartTime() / 30000).hashCode();
        result = 31 * result + Long.valueOf(timeWindow.getEndTime() / 30000).hashCode();
        return result;
    }

    @JsonIgnore
    public TimeWindow getWindow() {
        return WindowUtil.calculate(currentTime, duration);
    }

    @Override
    public String toString() {
        return "WindowFilter{" +
                "field=" + getField() +
                ", currentTime=" + currentTime +
                ", duration=" + duration +
                '}';
    }

    @Override
    public boolean isFilterTemporal() {
        return true;
    }
}
