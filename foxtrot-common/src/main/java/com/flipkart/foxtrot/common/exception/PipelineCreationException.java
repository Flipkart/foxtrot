package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class PipelineCreationException extends FoxtrotException {

    private final String reason;

    protected PipelineCreationException(String reason) {
        super(ErrorCode.PIPELINE_NOT_CREATED);
        this.reason = reason;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", reason);
        return map;
    }
}
