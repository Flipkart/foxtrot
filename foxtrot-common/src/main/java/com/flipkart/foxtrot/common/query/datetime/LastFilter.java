package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.google.common.base.Strings;
import io.dropwizard.util.Duration;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper =  true)
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
    public LastFilter(String field,
                      long currentTime,
                      Duration duration,
                      RoundingMode roundingMode) {
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

    public void setDuration(Duration duration) {
        this.duration = duration;
        this.roundingMode = roundingMode == null
                            ? RoundingMode.NONE
                            : roundingMode;
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    @JsonIgnore
    public boolean isFilterTemporal() {
        return true;
    }

    @JsonIgnore
    public TimeWindow getWindow() {
        return WindowUtil.calculate(currentTime, duration, roundingMode);
    }

}
