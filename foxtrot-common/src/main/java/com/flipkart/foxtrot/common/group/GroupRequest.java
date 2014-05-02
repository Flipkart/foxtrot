package com.flipkart.foxtrot.common.group;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 4:52 PM
 */
public class GroupRequest implements ActionRequest {
    @NotNull
    @NotEmpty
    private String table;

    private List<Filter> filters;

    @NotNull
    @NotEmpty
    private List<String> nesting;

    public GroupRequest() {
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public List<String> getNesting() {
        return nesting;
    }

    public void setNesting(List<String> nesting) {
        this.nesting = nesting;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
