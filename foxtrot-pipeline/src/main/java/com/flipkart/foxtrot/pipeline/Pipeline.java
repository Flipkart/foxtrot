package com.flipkart.foxtrot.pipeline;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.google.common.base.Strings;
import io.dropwizard.validation.ValidationMethod;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

@Getter
@Setter
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class Pipeline implements Serializable {

    @JsonIgnore
    private static final long serialVersionUID = -3086868483579298018L;
    @NotEmpty
    private String name;
    @NotEmpty
    private List<ProcessorDefinition> processors;
    private boolean ignoreErrors;
    private PipelineExecutionMode executionMode = PipelineExecutionMode.SERIAL;

    @ValidationMethod(message = "Name may only contain Uppercase letters and hyphen.")
    @JsonIgnore
    public boolean isValidName() {
        return !Strings.isNullOrEmpty(name) && Pattern.compile("^[A-Z-]+$").matcher(name).matches();
    }

}
