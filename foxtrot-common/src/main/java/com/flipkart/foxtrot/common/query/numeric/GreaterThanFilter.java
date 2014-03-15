package com.flipkart.foxtrot.common.query.numeric;

import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.FilterOperator;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 8:17 PM
 */
public class GreaterThanFilter extends NumericBinaryFilter {
    public GreaterThanFilter() {
        super(FilterOperator.greater_than);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
