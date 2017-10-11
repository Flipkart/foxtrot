package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import io.dropwizard.util.Duration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.validation.constraints.NotNull;

public class LastFilter extends Filter {

    private long currentTime;

    private RoundingMode roundingMode;

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

    public RoundingMode getRoundingMode() {
        return roundingMode;
    }

    public void setRoundingMode(RoundingMode roundingMode) {
        this.roundingMode = roundingMode;
    }

    @JsonIgnore
    public TimeWindow getWindow() {
        return WindowUtil.calculate(currentTime, duration, roundingMode);
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

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LastFilter rhs = (LastFilter) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(this.currentTime, rhs.currentTime)
                .append(this.roundingMode, rhs.roundingMode)
                .append(this.duration, rhs.duration)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(currentTime)
                .append(roundingMode)
                .append(duration)
                .toHashCode();
    }
}
