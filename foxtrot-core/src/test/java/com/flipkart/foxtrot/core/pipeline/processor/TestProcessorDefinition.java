package com.flipkart.foxtrot.core.pipeline.processor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class TestProcessorDefinition extends ProcessorDefinition {

    @JsonIgnore
    private static final long serialVersionUID = -3086868483579298017L;
    private String matchField;

    @Builder
    public TestProcessorDefinition(String matchField) {
        super("TEST");
        this.matchField = matchField;
    }

    public TestProcessorDefinition() {
        super("TEST");
    }
}
