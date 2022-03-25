package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class PipelineMissingException extends FoxtrotException {

    private final String pipeline;

    protected PipelineMissingException(String pipeline) {
        super(ErrorCode.PIPELINE_NOT_FOUND);
        this.pipeline = pipeline;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("pipeline", this.pipeline);
        return map;
    }
}