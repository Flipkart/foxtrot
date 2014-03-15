package com.flipkart.foxtrot.common.query.string;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 3:46 PM
 */
public class ContainsFilter extends Filter {
    @NotNull
    private String expression;

    public ContainsFilter() {
        super(FilterOperator.contains);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
