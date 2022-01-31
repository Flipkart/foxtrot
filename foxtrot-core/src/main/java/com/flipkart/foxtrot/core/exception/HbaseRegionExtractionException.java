package com.flipkart.foxtrot.core.exception;

import java.util.Map;

public class HbaseRegionExtractionException extends FoxtrotException {
    private final String message;

    public HbaseRegionExtractionException(String message, Throwable cause) {
        super(ErrorCode.HBASE_REGIONS_EXTRACTION_FAILURE, message, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = super.toMap();
        map.put("message", message);
        return map;
    }
}
