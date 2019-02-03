package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class DataCleanupException extends FoxtrotException {

    private String message;

    protected DataCleanupException(String message, Throwable cause) {
        super(ErrorCode.DATA_CLEANUP_ERROR, cause);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
