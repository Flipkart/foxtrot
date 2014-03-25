package com.flipkart.foxtrot.common.query;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 6:38 PM
 */
public class Query implements CachableResponseGenerator {
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

    @Override
    public String getCachekey() {
        long filterHashKey = 0L;
        for(Filter filter : filters) {
            filterHashKey += 31 * filter.hashCode();
        }
        final String key = String.format("%s-%d-%d-%d", table, from, limit, filterHashKey);

        return UUID.nameUUIDFromBytes(key.getBytes()).toString();
    }

}
