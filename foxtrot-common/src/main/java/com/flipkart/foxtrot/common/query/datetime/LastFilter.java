package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.google.common.base.Strings;
import io.dropwizard.util.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotNull;

@Data
@ToString(callSuper = true)
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

    @Builder
    public LastFilter(String field, long currentTime, Duration duration, RoundingMode roundingMode) {
        super(FilterOperator.last);
        super.setField(Strings.isNullOrEmpty(field) ? "_timestamp" : field);
        this.currentTime = currentTime == 0 ? System.currentTimeMillis() : currentTime;
        this.duration = duration;
        this.roundingMode = roundingMode == null ? RoundingMode.NONE : roundingMode;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
        this.roundingMode = roundingMode == null ? RoundingMode.NONE : roundingMode;
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @JsonIgnore
    public TimeWindow getWindow() {
        return WindowUtil.calculate(currentTime, duration, roundingMode);
    }

    @Override
    public int hashCode() {
        int result = getOperator().hashCode();
        result = 31 * result + getField().hashCode();
        if(!getField().equals("_timestamp")) {
            result = result * 21 + (getCurrentTime() == 0
                                    ? 43
                                    : Long.valueOf(getCurrentTime()).hashCode());
        }
        else {
            result = result * 21 + Long.valueOf(getCurrentTime() / (long) 30000).hashCode();
        }
        result = result * 13 + getRoundingMode().hashCode();
        result = result * 7 + getDuration().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        else if(!(o instanceof LastFilter)) {
            return false;
        }

        LastFilter that = (LastFilter) o;

        return getField().equals(that.getField()) && getOperator().equals(that.getOperator()) &&
                getDuration().equals(that.getDuration()) && getRoundingMode().equals(that.getRoundingMode()) &&
                getCurrentTime() == that.getCurrentTime();
    }

}
