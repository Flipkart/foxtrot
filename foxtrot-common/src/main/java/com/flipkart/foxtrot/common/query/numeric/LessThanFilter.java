package com.flipkart.foxtrot.common.query.numeric;

import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 8:17 PM
 */
public class LessThanFilter extends NumericBinaryFilter {
    public LessThanFilter() {
        super(FilterOperator.less_than);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
