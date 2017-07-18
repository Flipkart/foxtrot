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
public class CardinalityEstimationData extends EstimationData {
    private static final long serialVersionUID = -7107697070895705011L;
    private long cardinality;

    public CardinalityEstimationData() {
        super(EstimationDataType.CARDINALITY);
    }

    @Builder
    public CardinalityEstimationData(long cardinality, long count) {
        super(EstimationDataType.CARDINALITY, count);
        this.cardinality = cardinality;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
