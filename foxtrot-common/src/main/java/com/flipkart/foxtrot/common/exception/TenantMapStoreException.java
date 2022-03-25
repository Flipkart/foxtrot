package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

public class TenantMapStoreException extends FoxtrotException {

    private final String message;

    public TenantMapStoreException(String message,
                                   Throwable cause) {
        super(ErrorCode.TENANT_MAP_STORE_ERROR, message, cause);
        this.message = message;
    }

    public TenantMapStoreException(String message) {
        super(ErrorCode.TENANT_MAP_STORE_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}