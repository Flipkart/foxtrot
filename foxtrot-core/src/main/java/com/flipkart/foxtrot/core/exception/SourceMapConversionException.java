package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by mudit.g on May, 2019
 ***/
public class SourceMapConversionException extends FoxtrotException {

    private final String message;

    public SourceMapConversionException(String message, Throwable cause) {
        super(ErrorCode.SOURCE_MAP_CONVERSION_FAILURE, message, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
