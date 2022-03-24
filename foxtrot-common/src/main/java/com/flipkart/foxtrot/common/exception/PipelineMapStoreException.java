package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.Map;

public class PipelineMapStoreException extends FoxtrotException {

    private final String message;

    public PipelineMapStoreException(String message,
                                     Throwable cause) {
        super(ErrorCode.PIPELINE_MAP_STORE_ERROR, message, cause);
        this.message = message;
    }

    public PipelineMapStoreException(String message) {
        super(ErrorCode.PIPELINE_MAP_STORE_ERROR, message);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", message);
        map.put("rootCause", ExceptionUtils.getRootCause(getCause()) == null
                ? null
                : ExceptionUtils.getRootCause(getCause())
                .getMessage());
        return map;
    }
}