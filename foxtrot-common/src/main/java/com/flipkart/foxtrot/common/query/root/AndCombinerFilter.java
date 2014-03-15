package com.flipkart.foxtrot.common.query.root;

import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.FilterOperator;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 4:13 PM
 */
public class AndCombinerFilter extends CombinerFilter {
    public AndCombinerFilter() {
        super(FilterOperator.and);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
