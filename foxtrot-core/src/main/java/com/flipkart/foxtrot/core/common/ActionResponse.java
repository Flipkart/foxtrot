package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.core.querystore.actions.QueryResponse;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 9:17 PM
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opcode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = QueryResponse.class, name= "query")
})

public interface ActionResponse {
}
