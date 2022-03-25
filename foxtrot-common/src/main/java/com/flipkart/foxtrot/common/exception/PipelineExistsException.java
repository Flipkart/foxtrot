package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class PipelineExistsException extends FoxtrotException {

    private final String pipelineName;

    protected PipelineExistsException(String pipelineName) {
        super(ErrorCode.PIPELINE_ALREADY_EXISTS);
        this.pipelineName = pipelineName;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("pipeline", this.pipelineName);
        return map;
    }
}
