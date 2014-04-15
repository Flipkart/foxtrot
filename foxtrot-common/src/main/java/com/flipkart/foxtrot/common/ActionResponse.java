package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 9:17 PM
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opcode")
public interface ActionResponse {
}
