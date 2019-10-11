package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by mudit.g on May, 2019
 ***/
public class FqlParsingException extends FoxtrotException {

    private final String message;

    public FqlParsingException(String message, Throwable cause) {
        super(ErrorCode.FQL_PARSE_ERROR, message, cause);
        this.message = message;
    }

    public FqlParsingException(String message) {
        super(ErrorCode.FQL_PARSE_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
