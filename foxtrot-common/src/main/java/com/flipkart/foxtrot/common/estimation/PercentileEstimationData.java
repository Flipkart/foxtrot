package com.flipkart.foxtrot.common.estimation;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PercentileEstimationData extends EstimationData {
    private static final long serialVersionUID = -4790803356348252020L;

    private double[] values;

    public PercentileEstimationData() {
        super(EstimationDataType.PERCENTILE);
    }

    @Builder
    public PercentileEstimationData(double[] values, long count) {
        super(EstimationDataType.PERCENTILE, count);
        this.values = values;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
