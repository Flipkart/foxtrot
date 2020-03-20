package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.Map;

public class HbaseRegionMergeException extends FoxtrotException {
    private final String message;

    public HbaseRegionMergeException(String message, Throwable cause) {
        super(ErrorCode.HBASE_REGIONS_MERGE_FAILURE, message, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        return map;
    }
}
