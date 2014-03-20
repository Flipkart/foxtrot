package com.flipkart.foxtrot.common.histogram;

import com.flipkart.foxtrot.common.query.Filter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:06 AM
 */
public class HistogramRequest {
    @NotNull
    @NotEmpty
    private String table;

    private List<Filter> filters;
    @Min(0)
    private long from;
    @Min(0)
    private long to;

    @NotNull
    @NotEmpty
    private String field;

    private Period period;

    public HistogramRequest() {
        this.filters = Collections.emptyList();
        long timestamp = System.currentTimeMillis();
        this.from = timestamp - 86400000;
        this.to = timestamp;
        this.field = "timestamp";
        this.period = Period.minutes;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
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

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
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
}
