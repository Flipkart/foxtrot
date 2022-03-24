package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

public class NodeGroupExecutionException extends FoxtrotException {

    private final String message;

    public NodeGroupExecutionException(String message,
                                       Throwable cause) {
        super(ErrorCode.NODE_GROUP_EXECUTION_ERROR, message, cause);
        this.message = message;
    }

    public NodeGroupExecutionException(String message) {
        super(ErrorCode.NODE_GROUP_EXECUTION_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
