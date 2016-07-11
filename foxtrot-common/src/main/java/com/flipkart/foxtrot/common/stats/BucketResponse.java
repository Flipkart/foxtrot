package com.flipkart.foxtrot.common.stats;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

public class BucketResponse<T> {

    private String key;
    private T result;
    private List<BucketResponse<T>> buckets = Lists.newArrayList();

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public List<BucketResponse<T>> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<BucketResponse<T>> buckets) {
        this.buckets = buckets;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("key", key)
                .append("result", result)
                .append("buckets", buckets)
                .toString();
    }
}
