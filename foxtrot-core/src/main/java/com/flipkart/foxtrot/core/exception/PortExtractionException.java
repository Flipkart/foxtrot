package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by mudit.g on May, 2019
 ***/
public class PortExtractionException extends FoxtrotException {

    private final String message;

    public PortExtractionException(String message) {
        super(ErrorCode.PORT_EXTRACTION_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
