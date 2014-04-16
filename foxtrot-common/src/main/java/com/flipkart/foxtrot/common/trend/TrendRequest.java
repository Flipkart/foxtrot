package com.flipkart.foxtrot.common.trend;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.google.common.collect.Lists;
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

    private List<Filter> filters = Lists.newArrayList();

    private String field = "all";

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
}
