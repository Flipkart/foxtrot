package com.flipkart.foxtrot.pipeline.processors.geo;

import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.flipkart.foxtrot.pipeline.validator.ValidJsonPath;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class RegionMatchProcessorDefinition extends ProcessorDefinition {

    @NotEmpty
    @ValidJsonPath(definite = true)
    private String matchField;

    @NotEmpty
    @ValidJsonPath(definite = true)
    private Map<String, String> targetFieldRootMapping;

    @NotEmpty
    private String geoJsonSource;

    private String cacheSpec;
    private int cacheS2Level;

    @Builder
    public RegionMatchProcessorDefinition(String matchField,
                                          Map<String, String> targetFieldRootMapping,
                                          String geoJsonSource,
                                          String cacheSpec) {
        this();
        this.matchField = matchField;
        this.targetFieldRootMapping = targetFieldRootMapping;
        this.geoJsonSource = geoJsonSource;
        this.cacheSpec = cacheSpec;
    }

    public RegionMatchProcessorDefinition() {
        super("GEO::REGION_MATCHER");
    }


}
