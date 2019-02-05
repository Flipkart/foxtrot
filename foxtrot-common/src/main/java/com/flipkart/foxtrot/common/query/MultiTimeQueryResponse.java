package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;
import lombok.Data;

import java.util.Map;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class MultiTimeQueryResponse extends ActionResponse {

    private Map<String, ActionResponse> responses;

    public MultiTimeQueryResponse(Map<String, ActionResponse> responses) {
        super(Opcodes.MULTI_TIME_QUERY);
        this.responses = responses;
    }
    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }
}
