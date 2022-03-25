package com.flipkart.foxtrot.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Joiner;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@UtilityClass
public class PipelineUtils {

    private static Reflections reflections;

    public static Reflections getReflections() {
        return reflections;
    }

    public static void init(ObjectMapper mapper, Set<String> packages) {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        Set<String> scannerPaths = new HashSet(packages);
        scannerPaths.add("com.flipkart.foxtrot.pipeline.processors");
        configurationBuilder = configurationBuilder.forPackages(scannerPaths.toArray(new String[0]));
        log.info("Scanning Processor definitions in = [{}]", Joiner.on(",")
                .join(scannerPaths));
        reflections = new Reflections(configurationBuilder);

        reflections.getTypesAnnotatedWith(ProcessorMarker.class)
                .stream()
                .forEach(clazz -> mapper.registerSubtypes(new NamedType(
                        clazz.getDeclaredAnnotation(ProcessorMarker.class)
                                .processorClass(), clazz.getDeclaredAnnotation(ProcessorMarker.class)
                        .name())));
    }


}
