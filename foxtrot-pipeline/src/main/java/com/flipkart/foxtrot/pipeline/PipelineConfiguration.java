package com.flipkart.foxtrot.pipeline;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipelineConfiguration {

    @Builder.Default
    private String handlerCacheSpec;
    private Set<String> handlerProcessorPath = new HashSet<>();
}
