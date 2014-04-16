package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

public class AnyFilter extends Filter {

    public AnyFilter() {
        super(FilterOperator.any);
    }

    public AnyFilter(String field) {
        super(FilterOperator.any, field);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EqualsFilter that = (EqualsFilter) o;

        return getField().equals(that.getField());

    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

