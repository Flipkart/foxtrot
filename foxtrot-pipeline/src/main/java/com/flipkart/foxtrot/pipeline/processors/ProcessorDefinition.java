package com.flipkart.foxtrot.pipeline.processors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
public class ProcessorDefinition implements Serializable {

    @JsonIgnore
    private static final long serialVersionUID = -3086868483579298016L;
    private String type;

    public ProcessorDefinition() {
    }

    public ProcessorDefinition(String type) {
        this.type = type;
    }
}
