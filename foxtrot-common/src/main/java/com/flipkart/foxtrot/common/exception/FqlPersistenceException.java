package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by mudit.g on May, 2019
 ***/
public class FqlPersistenceException extends FoxtrotException {

    private final String message;

    public FqlPersistenceException(String message,
                                   Throwable cause) {
        super(ErrorCode.FQL_PERSISTENCE_EXCEPTION, message, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
