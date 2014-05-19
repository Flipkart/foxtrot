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

    private boolean temporal = false;

    protected NumericBinaryFilter(final String operator) {
        super(operator);
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NumericBinaryFilter that = (NumericBinaryFilter) o;

        return value.equals(that.value);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if(!temporal) {
            result = 31 * result + value.hashCode();
        }
        else {
            result = 31 * result + Long.valueOf(value.longValue()/30000).hashCode();
        }
        return result;
    }

    public boolean isTemporal() {
        return temporal;
    }

    public void setTemporal(boolean temporal) {
        this.temporal = temporal;
    }
}
