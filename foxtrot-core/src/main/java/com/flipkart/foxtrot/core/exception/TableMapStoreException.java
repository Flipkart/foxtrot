package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by mudit.g on May, 2019
 ***/
public class TableMapStoreException extends FoxtrotException {

    private final String message;

    public TableMapStoreException(String message, Throwable cause) {
        super(ErrorCode.TABLE_MAP_STORE_ERROR, message, cause);
        this.message = message;
    }

    public TableMapStoreException(String message) {
        super(ErrorCode.TABLE_MAP_STORE_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
