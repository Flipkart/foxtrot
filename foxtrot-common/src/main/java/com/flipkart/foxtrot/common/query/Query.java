package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionRequest;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 6:38 PM
 */
public class Query implements ActionRequest {
    @NotNull
    private String table;

    @NotNull
    private List<Filter> filters;

    @NotNull
    private FilterCombinerType combiner = FilterCombinerType.and;

    private ResultSort sort;

    @Min(0)
    private int from = 0;

    @Min(10)
    private int limit = 10;

    public Query() {
        this.sort = new ResultSort();
        this.sort.setField("_timestamp");
        this.sort.setOrder(ResultSort.Order.desc);
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
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

    public FilterCombinerType getCombiner() {
        return combiner;
    }

    public void setCombiner(FilterCombinerType combiner) {
        this.combiner = combiner;
    }

}
