package com.flipkart.foxtrot.common.query.root;

import com.flipkart.foxtrot.common.query.Filter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 4:14 PM
 */
public abstract class CombinerFilter extends Filter {
    @NotNull
    @NotEmpty
    private List<Filter> filters;

    protected CombinerFilter(final String operator) {
        super(operator);
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }
}
