package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendRequest implements ActionRequest {
    @NotNull
    @NotEmpty
    private String table;

    @NotNull
    @NotEmpty
    private String field;

    @NotNull
    private List<Filter> filters;

    @NotNull
    private FilterCombinerType combiner = FilterCombinerType.and;

    @NotNull
    private Period period = Period.hours;

    private String timestamp = "_timestamp";

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long from = 0L;

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long to = 0L;

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

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
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

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "StatsTrendRequest{" +
                "table='" + table + '\'' +
                ", field='" + field + '\'' +
                ", filters=" + filters +
                ", combiner=" + combiner +
                ", period=" + period +
                ", timestamp='" + timestamp + '\'' +
                ", from=" + from +
                ", to=" + to +
                '}';
    }
}
