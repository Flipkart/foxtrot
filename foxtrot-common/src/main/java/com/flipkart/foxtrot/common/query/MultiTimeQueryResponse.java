package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;
import lombok.Data;

import java.util.Map;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class MultiTimeQueryResponse extends ActionResponse {

    private Map<String, ActionResponse> responses;

    public MultiTimeQueryResponse(String opcode) {
        super(opcode);
    }
    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }
}
