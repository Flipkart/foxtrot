package com.flipkart.foxtrot.core.pipeline.impl;

import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.pipeline.PipelineManager;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.pipeline.Pipeline;

import javax.inject.Inject;
import java.util.List;

public class FoxtrotPipelineManager implements PipelineManager {

    private final PipelineMetadataManager metadataManager;

    @Inject
    public FoxtrotPipelineManager(PipelineMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public void save(Pipeline pipeline) {
        if (metadataManager.exists(pipeline.getName())) {
            throw FoxtrotExceptions.createPipelineExistsException(pipeline.getName());
        }
        metadataManager.save(pipeline);
    }

    @Override
    public Pipeline get(String pipelineName) {
        Pipeline pipeline = metadataManager.get(pipelineName);
        if (pipeline == null) {
            throw FoxtrotExceptions.createPipelineMissingException(pipelineName);
        }
        return pipeline;
    }

    @Override
    public List<Pipeline> getAll() {
        return metadataManager.get();
    }

    @Override
    public void update(Pipeline pipeline) {
        if (!metadataManager.exists(pipeline.getName())) {
            throw FoxtrotExceptions.createPipelineMissingException(pipeline.getName());
        }
        metadataManager.save(pipeline);
    }


}
