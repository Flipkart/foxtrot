package com.flipkart.foxtrot.common.group;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by swapnil on 08/01/16.
 */
public class MetricsAggregation {
    private MetricsAggragationType type;
    private String field;

    public MetricsAggragationType getType() {
        return type;
    }

    public void setType(MetricsAggragationType type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("type", type).append("field",field).toString();
    }

    public enum MetricsAggragationType {
        sum,max,min,avg,percentiles,stats
    }
}
