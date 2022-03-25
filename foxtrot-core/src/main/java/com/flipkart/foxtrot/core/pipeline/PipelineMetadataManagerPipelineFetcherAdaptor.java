package com.flipkart.foxtrot.core.pipeline;

import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.resolver.PipelineFetcher;
import com.google.inject.Inject;

public class PipelineMetadataManagerPipelineFetcherAdaptor implements PipelineFetcher {

    private final PipelineMetadataManager pipelineMetadataManager;

    @Inject
    public PipelineMetadataManagerPipelineFetcherAdaptor(PipelineMetadataManager pipelineMetadataManager) {
        this.pipelineMetadataManager = pipelineMetadataManager;
    }

    @Override
    public Pipeline fetch(String id) {
        return pipelineMetadataManager.get(id);
    }
}
