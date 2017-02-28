package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

/**
 * Created by rishabh.goyal on 07/08/14.
 */
public class StatsResponse extends ActionResponse {

    private StatsValue result;
    private List<BucketResponse<StatsValue>> buckets;

    public StatsResponse() {
        super(Opcodes.STATS);

    }

    public StatsResponse(StatsValue statsValue) {
        super(Opcodes.STATS);
        this.result = statsValue;
    }

    public StatsValue getResult() {
        return result;
    }

    public List<BucketResponse<StatsValue>> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<BucketResponse<StatsValue>> buckets) {
        this.buckets = buckets;
    }

    public void setResult(StatsValue result) {
        this.result = result;
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("result", result)
                .append("buckets", buckets)
                .toString();
    }
}
