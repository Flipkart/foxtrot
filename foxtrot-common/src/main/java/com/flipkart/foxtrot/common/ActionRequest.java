package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Query;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:49 PM
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opcode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Query.class, name= "query"),
        @JsonSubTypes.Type(value = GroupRequest.class, name= "group")
})
public interface ActionRequest {
}
