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

    private static final long serialVersionUID = 9133737808349090291L;

    public FixedEstimationData() {
        super(EstimationDataType.FIXED);
    }

    @Builder
    public FixedEstimationData(long count) {
        super(EstimationDataType.FIXED, count);
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
