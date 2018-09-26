package com.flipkart.foxtrot.common.estimation;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;

/**
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PercentileEstimationData extends EstimationData {
    private static final long serialVersionUID = -4790803356348252020L;

    private double[] values;
    private long cardinality;

    public PercentileEstimationData() {
        super(EstimationDataType.PERCENTILE);
    }

    public double[] getValues() {
        return Arrays.copyOf(values, values.length);
    }

    @Builder
    public PercentileEstimationData(double[] values, long count, long cardinality) {
        super(EstimationDataType.PERCENTILE, count);
        this.values = values;
        this.cardinality = cardinality;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
