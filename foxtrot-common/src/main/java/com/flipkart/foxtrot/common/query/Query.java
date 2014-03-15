package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.root.CombinerFilter;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 6:38 PM
 */
public class Query {
    @NotNull
    private String table;

    @NotNull
    private CombinerFilter filter;

    private ResultSort sort;

    @Min(0)
    private int from = 0;

    @Min(10)
    private int limit = 0;

    public Query() {
    }

    public CombinerFilter getFilter() {
        return filter;
    }

    public void setFilter(CombinerFilter filter) {
        this.filter = filter;
    }

    public ResultSort getSort() {
        return sort;
    }

    public void setSort(ResultSort sort) {
        this.sort = sort;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public static class Sort {
    }

}
