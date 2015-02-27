package com.flipkart.foxtrot.common.top;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by rishabh.goyal on 28/02/15.
 */
public class ValueCount {

    private String value;
    private long count;

    public ValueCount() {
    }

    public ValueCount(String value, long count) {
        this.value = value;
        this.count = count;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value)
                .append("count", count)
                .toString();
    }
}
