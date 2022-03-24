package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

public class NodeGroupStoreException extends FoxtrotException {

    private final String message;

    public NodeGroupStoreException(String message,
                                   Throwable cause) {
        super(ErrorCode.NODE_GROUP_STORE_ERROR, message, cause);
        this.message = message;
    }

    public NodeGroupStoreException(String message) {
        super(ErrorCode.NODE_GROUP_STORE_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
