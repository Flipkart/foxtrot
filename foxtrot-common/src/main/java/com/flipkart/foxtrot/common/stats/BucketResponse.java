package com.flipkart.foxtrot.common.stats;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

public class BucketResponse {

    private String key;
    private StatsValue result;
    private List<BucketResponse> buckets = Lists.newArrayList();

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public StatsValue getResult() {
        return result;
    }

    public void setResult(StatsValue result) {
        this.result = result;
    }

    public List<BucketResponse> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<BucketResponse> buckets) {
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
