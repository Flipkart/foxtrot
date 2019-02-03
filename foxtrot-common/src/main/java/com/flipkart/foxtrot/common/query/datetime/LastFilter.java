package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.google.common.base.Strings;
import io.dropwizard.util.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.constraints.NotNull;

@Data
@EqualsAndHashCode(callSuper = true)
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
        super.setField(Strings.isNullOrEmpty(field)
                        ? "_timestamp"
                        : field);
        this.currentTime = currentTime == 0
                ? System.currentTimeMillis()
                : currentTime;
        this.duration = duration;
        this.roundingMode = roundingMode == null
                ? RoundingMode.NONE
                : roundingMode;
    }

    @Override
    public<T> T accept(FilterVisitor<T> visitor) throws Exception {
        return visitor.visit(this);
    }

    @JsonIgnore
    public TimeWindow getWindow() {
        return WindowUtil.calculate(currentTime, duration, roundingMode);
    }

    @Override
    @JsonIgnore
    public boolean isFilterTemporal() {
        return true;
    }

}
