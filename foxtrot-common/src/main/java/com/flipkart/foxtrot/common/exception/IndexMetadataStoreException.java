package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;


public class IndexMetadataStoreException extends FoxtrotException {

    private final String message;

    public IndexMetadataStoreException(String message,
                                       Throwable cause) {
        super(ErrorCode.INDEX_METADATA_STORE_EXCEPTION, message, cause);
        this.message = message;
    }

    public IndexMetadataStoreException(String message) {
        super(ErrorCode.INDEX_METADATA_STORE_EXCEPTION, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
