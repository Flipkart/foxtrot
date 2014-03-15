package com.flipkart.foxtrot.common.query.root;

import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.FilterOperator;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 4:13 PM
 */
public class OrCombinerFilter extends CombinerFilter {
    public OrCombinerFilter() {
        super(FilterOperator.or);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
