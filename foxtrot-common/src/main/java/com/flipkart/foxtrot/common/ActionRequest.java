package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:49 PM
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opcode")
public interface ActionRequest {
}
