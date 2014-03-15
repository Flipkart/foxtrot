package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.FilterOperator;

import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 3:35 PM
 */
public class NotEqualsFilter extends Filter {
    @NotNull
    private Object value;

    public NotEqualsFilter() {
        super(FilterOperator.not_equals);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
