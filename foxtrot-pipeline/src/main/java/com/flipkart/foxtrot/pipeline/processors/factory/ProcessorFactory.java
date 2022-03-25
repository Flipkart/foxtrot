package com.flipkart.foxtrot.pipeline.processors.factory;

import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;

public interface ProcessorFactory {
    Processor create(ProcessorDefinition process) throws ProcessorCreationException;
}
