package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendRequest extends ActionRequest {
    @NotNull
    @NotEmpty
    private String table;

    @NotNull
    @NotEmpty
    private String field;

    @NotNull
    private FilterCombinerType combiner = FilterCombinerType.and;

    @NotNull
    private Period period = Period.hours;

    private String timestamp = "_timestamp";

    public StatsTrendRequest() {

    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public FilterCombinerType getCombiner() {
        return combiner;
    }

    public void setCombiner(FilterCombinerType combiner) {
        this.combiner = combiner;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "StatsTrendRequest{" +
                "table='" + table + '\'' +
                ", field='" + field + '\'' +
                ", filters=" + getFilters() +
                ", combiner=" + combiner +
                ", period=" + period +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
