package com.flipkart.foxtrot.core.pipeline;

import com.flipkart.foxtrot.pipeline.Pipeline;

import java.util.List;

public interface PipelineManager {

    void save(Pipeline pipeline);

    Pipeline get(String pipelineId);

    List<Pipeline> getAll();

    void update(Pipeline pipeline);
}
