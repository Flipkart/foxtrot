package com.flipkart.foxtrot.pipeline.processors.factory;

import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.ProcessorMarker;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class ReflectionBasedProcessorFactory implements ProcessorFactory {

    private Injector injector;

    @Inject
    public ReflectionBasedProcessorFactory(Injector injector) {
        this.injector = injector;
    }

    @Override
    public Processor create(ProcessorDefinition process) throws ProcessorCreationException {
        Set<Class<?>> subTypesOf = PipelineUtils.getReflections()
                .getTypesAnnotatedWith(ProcessorMarker.class);
        log.info("finding action of type: " + process.getType());
        Class<?> actionSubClass = null;
        Iterator var4 = subTypesOf.iterator();

        while (var4.hasNext()) {
            Class<?> clazz = (Class) var4.next();
            if (clazz.getAnnotation(ProcessorMarker.class)
                    .name()
                    .equals(process.getType())) {
                actionSubClass = clazz;
            }
        }

        if (Objects.isNull(actionSubClass)) {
            throw new IllegalArgumentException("there is no action of type: " + process.getType());
        } else {
            log.info("initiating action: " + process.getType());
            try {
                Processor action = (Processor) actionSubClass.newInstance();
                action.setProcessorDefinition(process);
                this.injector.injectMembers(action);
                action.init();
                return action;
            } catch (Exception e) {
                throw new ProcessorCreationException("Exception creating processor through Reflection", e);
            }
        }
    }
}
