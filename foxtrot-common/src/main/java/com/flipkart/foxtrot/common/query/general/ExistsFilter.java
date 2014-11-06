package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

/**
 * Created by rishabh.goyal on 03/11/14.
 */
public class ExistsFilter extends Filter {

    public ExistsFilter() {
        super(FilterOperator.exists);
    }

    public ExistsFilter(String field) {
        super(FilterOperator.exists, field);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
