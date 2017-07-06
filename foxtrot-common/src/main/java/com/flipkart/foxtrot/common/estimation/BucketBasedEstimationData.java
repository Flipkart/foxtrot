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
public class BucketBasedEstimationData extends EstimationData {
    private double[] values;

    public BucketBasedEstimationData() {
        super(EstimationDataType.BUCKET_BASED);
    }

    @Builder
    public BucketBasedEstimationData(double[] values, long count) {
        super(EstimationDataType.BUCKET_BASED, count);
        this.values = values;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
