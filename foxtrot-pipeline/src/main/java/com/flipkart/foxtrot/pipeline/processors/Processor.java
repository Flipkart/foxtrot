package com.flipkart.foxtrot.pipeline.processors;

import com.flipkart.foxtrot.common.Document;

public interface Processor<T extends ProcessorDefinition> {

    void setProcessorDefinition(T processor);

    default void init() throws ProcessorInitializationException {

    }

    void handle(Document document);
}
