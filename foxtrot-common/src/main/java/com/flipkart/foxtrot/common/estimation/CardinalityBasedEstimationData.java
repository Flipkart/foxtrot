package com.flipkart.foxtrot.common.estimation;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Estimated cardinality data
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class CardinalityBasedEstimationData extends EstimationData {
    private long cardinality;

    public CardinalityBasedEstimationData() {
        super(EstimationDataType.CARDINALITY_BASED);
    }

    @Builder
    public CardinalityBasedEstimationData(long cardinality, long count) {
        super(EstimationDataType.CARDINALITY_BASED, count);
        this.cardinality = cardinality;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
