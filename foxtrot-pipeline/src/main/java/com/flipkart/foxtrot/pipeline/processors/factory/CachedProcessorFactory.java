package com.flipkart.foxtrot.pipeline.processors.factory;

import com.flipkart.foxtrot.pipeline.PipelineConstants;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class CachedProcessorFactory implements ProcessorFactory {

    private LoadingCache<ProcessorDefinition, Processor> handlerCache;

    @Inject
    public CachedProcessorFactory(@Named(PipelineConstants.REFLECTION_BASED_PROCESSOR_FACTORY) ProcessorFactory delegate, @Named(PipelineConstants.PROCESSOR_CACHE_BUILDER) CacheBuilder caheBuilder) {
        this.handlerCache = caheBuilder.build(new CacheLoader<ProcessorDefinition, Processor>() {
            @Override
            public Processor load(ProcessorDefinition processorDefinition) throws Exception {
                val processorHandler = delegate.create(processorDefinition);
                processorHandler.init();
                return processorHandler;
            }
        });
    }

    @Override
    public Processor create(ProcessorDefinition process) throws ProcessorCreationException {
        try {
            return handlerCache.get(process);
        } catch (Exception e) {
            log.error("Error Creating processor", e);
            throw new ProcessorCreationException("Exception while creating processor", e);
        }
    }
}
