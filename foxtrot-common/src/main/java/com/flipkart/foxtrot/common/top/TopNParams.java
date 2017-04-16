package com.flipkart.foxtrot.common.top;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * Created by rishabh.goyal on 28/02/15.
 */
public class TopNParams {

    @NotNull
    @NotEmpty
    private String field;

    private boolean approxCount = false;

    private int count;

    public TopNParams() {
    }

    public TopNParams(String field, boolean approxCount, int count) {
        this.field = field;
        this.approxCount = approxCount;
        this.count = count;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public boolean isApproxCount() {
        return approxCount;
    }

    public void setApproxCount(boolean approxCount) {
        this.approxCount = approxCount;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("field", field)
                .append("approxCount", approxCount)
                .append("count", count)
                .toString();
    }
}
