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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricsAggregation)) return false;

        MetricsAggregation that = (MetricsAggregation) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (field != null ? field.hashCode() : 0);
        return result;
    }
}
