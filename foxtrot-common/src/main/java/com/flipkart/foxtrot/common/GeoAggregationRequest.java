package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.stats.Stat;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeoAggregationRequest extends ActionRequest {

    private String uniqueCountOn;

    private String aggregationField;

    private Stat aggregationType;

    @NotNull
    @NotEmpty
    private String locationField;

    private String consoleId;

    @Min(1)
    @Max(5)
    @NotNull
    private Integer gridLevel;
    @NotNull
    @NotEmpty
    private String table;

    public GeoAggregationRequest() {
        super(Opcodes.GEO_AGGREGATION);
    }

    @Builder
    public GeoAggregationRequest(List<Filter> filters,
                                 boolean bypassCache,
                                 Map<String, String> requestTags,
                                 SourceType sourceType,
                                 boolean extrapolationFlag,
                                 String uniqueCountOn,
                                 String aggregationField,
                                 Stat aggregationType,
                                 String locationField,
                                 String consoleId,
                                 String table,
                                 Integer gridLevel) {
        super(Opcodes.GEO_AGGREGATION, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.uniqueCountOn = uniqueCountOn;
        this.aggregationField = aggregationField;
        this.aggregationType = aggregationType;
        this.locationField = locationField;
        this.consoleId = consoleId;
        this.table = table;
        this.gridLevel = gridLevel;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("aggregationType", aggregationType)
                .append("uniqueCountOn", uniqueCountOn)
                .append("aggregationField", aggregationField)
                .append("consoleId", consoleId)
                .append("locationField", locationField)
                .append("gridLevel", gridLevel)
                .toString();
    }

    @Override
    public <T> T accept(ActionRequestVisitor<T> var1) {
        return var1.visit(this);
    }
}
