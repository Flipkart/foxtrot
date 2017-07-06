package com.flipkart.foxtrot.common.estimation;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Fixed estimation data
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class FixedEstimationData extends EstimationData {

    private long probability;

    public FixedEstimationData() {
        super(EstimationDataType.FIXED);
    }

    @Builder
    public FixedEstimationData(long probability, long count) {
        super(EstimationDataType.FIXED, count);
        this.probability = probability;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
