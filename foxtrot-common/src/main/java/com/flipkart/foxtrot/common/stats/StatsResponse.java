package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;

/**
 * Created by rishabh.goyal on 07/08/14.
 */
public class StatsResponse implements ActionResponse {

    private StatsValue result;

    public StatsResponse() {

    }

    public StatsResponse(StatsValue statsValue) {
        this.result = statsValue;
    }

    public StatsValue getResult() {
        return result;
    }

    public void setResult(StatsValue result) {
        this.result = result;
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }

}
