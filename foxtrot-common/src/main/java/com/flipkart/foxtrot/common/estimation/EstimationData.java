package com.flipkart.foxtrot.common.estimation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;

/**
 * Type specific estimation data
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "FIXED", value = FixedEstimationData.class),
})
public abstract class EstimationData {

    @Getter
    private final EstimationDataType type;

    private long count;

    protected EstimationData(EstimationDataType type) {
        this.type = type;
    }

    protected EstimationData(EstimationDataType type, long count) {
        this.type = type;
        this.count = count;
    }

    abstract public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor);

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
