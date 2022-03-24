package com.flipkart.foxtrot.pipeline.processors.string;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class StringLowerProcessorDefinition extends ProcessorDefinition {

    @JsonIgnore
    private static final long serialVersionUID = -3086868483579298017L;
    private String matchField;

    @Builder
    public StringLowerProcessorDefinition(String matchField) {
        this();
        this.matchField = matchField;
    }

    public StringLowerProcessorDefinition() {
        super("STR::LOWER");
    }
}
