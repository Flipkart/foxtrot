package com.flipkart.foxtrot.common.count;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
public class CountRequest implements ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String column;

    private List<Filter> filters;

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long from = 0L;

    @Min(0)
    @Max(Long.MAX_VALUE)
    private long to = 0L;

    public CountRequest() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
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

    @Override
    public String toString() {
        return "CountRequest{" +
                "table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", filters=" + filters +
                ", from=" + from +
                ", to=" + to +
                '}';
    }
}
