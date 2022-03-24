package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;

import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendResponse extends ActionResponse {

    private List<StatsTrendValue> result;
    private List<BucketResponse<List<StatsTrendValue>>> buckets;

    public StatsTrendResponse() {
        super(Opcodes.STATS_TREND);
    }

    public List<StatsTrendValue> getResult() {
        return result;
    }

    public void setResult(List<StatsTrendValue> result) {
        this.result = result;
    }

    public List<BucketResponse<List<StatsTrendValue>>> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<BucketResponse<List<StatsTrendValue>>> buckets) {
        this.buckets = buckets;
    }

    @Override
    public <T> T accept(ResponseVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
