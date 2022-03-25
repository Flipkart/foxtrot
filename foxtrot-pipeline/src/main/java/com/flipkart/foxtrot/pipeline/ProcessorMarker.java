package com.flipkart.foxtrot.pipeline;

import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ProcessorMarker {

    String name();

    Class<? extends ProcessorDefinition> processorClass();
}
