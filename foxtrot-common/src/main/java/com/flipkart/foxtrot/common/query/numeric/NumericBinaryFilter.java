package com.flipkart.foxtrot.common.query.numeric;

import com.flipkart.foxtrot.common.query.Filter;

import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:25 PM
 */
public abstract class NumericBinaryFilter extends Filter {
    @NotNull
    private Number value;

    protected NumericBinaryFilter(final String operator) {
        super(operator);
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }
}
