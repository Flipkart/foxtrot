package com.flipkart.foxtrot.core.pipeline;

import com.flipkart.foxtrot.pipeline.Pipeline;
import io.dropwizard.lifecycle.Managed;

import java.util.List;

public interface PipelineMetadataManager extends Managed {

    void save(Pipeline pipeline);

    Pipeline get(String pipelineId);

    List<Pipeline> get();

    boolean exists(String pipelineId);
}
