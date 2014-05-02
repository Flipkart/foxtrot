package com.flipkart.foxtrot.common.trend;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 2:30 PM
 */
public class TrendRequest implements ActionRequest {
    @NotNull
    @NotEmpty
    private String table;

    private List<Filter> filters;

    private String field = "all";

    private String timestamp = "_timestamp";

    private long interval = 86400000L;

    private List<String> values;

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long from = 0L;

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long to = 0L;

    public TrendRequest() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
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
        return new ToStringBuilder(this)
                .append("table", table)
                .append("filters", filters)
                .append("field", field)
                .append("values", values)
                .append("from", from)
                .append("to", to)
                .toString();
    }
}
