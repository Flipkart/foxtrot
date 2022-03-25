package com.flipkart.foxtrot.pipeline.processors.geo;

import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.TargetWriteMode;
import com.flipkart.foxtrot.pipeline.validator.ValidJsonPath;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Set;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class AddS2GridProcessorDefinition extends ProcessorDefinition {

    @NotEmpty
    @ValidJsonPath(definite = true)
    private String matchField;

    @NotEmpty
    @ValidJsonPath(definite = true)
    private String targetFieldRoot;

    @NotEmpty
    @Max(32)
    @Min(1)
    private Set<Integer> s2Levels;

    private TargetWriteMode targetWriteMode;

    @Builder
    public AddS2GridProcessorDefinition(String matchField,
                                        String targetFieldRoot,
                                        Set<Integer> s2Levels,
                                        TargetWriteMode targetWriteMode) {
        this();
        this.matchField = matchField;
        this.targetFieldRoot = targetFieldRoot;
        this.s2Levels = s2Levels;
        this.targetWriteMode = targetWriteMode;
    }

    public AddS2GridProcessorDefinition() {
        super("GEO::S2_GRID");
    }
}
