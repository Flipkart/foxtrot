package com.flipkart.foxtrot.common.estimation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * Type specific estimation data
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "FIXED", value = FixedEstimationData.class),
        @JsonSubTypes.Type(name = "CARDINALITY", value = CardinalityEstimationData.class),
        @JsonSubTypes.Type(name = "PERCENTILE", value = PercentileEstimationData.class)
})
@EqualsAndHashCode
@ToString
@Getter
@Setter
public abstract class EstimationData implements Serializable {

    private static final long serialVersionUID = -6542054750045180777L;

    private final EstimationDataType type;

    private long count;

    private Date lastEstimated;

    protected EstimationData(EstimationDataType type) {
        this.type = type;
    }

    protected EstimationData(EstimationDataType type, long count, Date lastEstimated) {
        this.type = type;
        this.count = count;
        this.lastEstimated = lastEstimated;
    }

    abstract public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor);
}
