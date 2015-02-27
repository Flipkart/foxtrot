package com.flipkart.foxtrot.common.top;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by rishabh.goyal on 28/02/15.
 */
public class TopNRequest implements ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private List<Filter> filters;

    @NotNull
    @NotEmpty
    private List<TopNParams> params;

    public TopNRequest() {
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

    public List<TopNParams> getParams() {
        return params;
    }

    public void setParams(List<TopNParams> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("filters", filters)
                .append("params", params)
                .toString();
    }
}
