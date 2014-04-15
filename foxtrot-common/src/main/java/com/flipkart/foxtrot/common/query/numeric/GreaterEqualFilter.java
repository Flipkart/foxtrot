package com.flipkart.foxtrot.common.query.numeric;

import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 8:17 PM
 */
public class GreaterEqualFilter extends NumericBinaryFilter {
    public GreaterEqualFilter() {
        super(FilterOperator.greater_equal);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
