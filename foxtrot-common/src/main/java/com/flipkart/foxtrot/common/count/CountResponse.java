package com.flipkart.foxtrot.common.count;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
public class CountResponse extends ActionResponse {

    private long count;

    public CountResponse() {
        super(Opcodes.COUNT);
    }

    public CountResponse(long count) {
        super(Opcodes.COUNT);
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public <T> T accept(ResponseVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
