package com.flipkart.foxtrot.pipeline;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.factory.ProcessorFactory;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PipelineExecutor {

    private ProcessorFactory processorFactory;

    @Inject
    public PipelineExecutor(ProcessorFactory processorFactory) {
        this.processorFactory = processorFactory;

    }

    @MonitoredFunction
    public Document execute(Pipeline pipeline,
                            Document document) throws PipelineExecutionException {
        Preconditions.checkNotNull(pipeline, "Pipeline should not be null");
        try {
            executeInternal(pipeline, document);
        } catch (Exception e) {
            if (!pipeline.isIgnoreErrors()) {
                throw new PipelineExecutionException("Error executing pipeline " + pipeline.getName(), e);
            }
            log.warn("Error executing pipeline {} {}. IgnoreError = true", pipeline.getName(), e.getStackTrace());
        }
        return document;
    }

    @MonitoredFunction
    private void executeInternal(Pipeline pipeline,
                                 Document document) throws ProcessorCreationException {
        for (ProcessorDefinition pipelineProcessor : pipeline.getProcessors()) {
            final Processor processorHandler = fetchOrCreateProcessor(pipelineProcessor);
            processorHandler.handle(document);
        }
    }

    @MonitoredFunction
    private Processor fetchOrCreateProcessor(ProcessorDefinition pipelineProcessor) throws ProcessorCreationException {
        return processorFactory.create(pipelineProcessor);
    }

}
