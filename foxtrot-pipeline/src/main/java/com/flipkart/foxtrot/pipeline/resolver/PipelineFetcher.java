package com.flipkart.foxtrot.pipeline.resolver;

import com.flipkart.foxtrot.pipeline.Pipeline;

public interface PipelineFetcher {
    Pipeline fetch(String id);
}
