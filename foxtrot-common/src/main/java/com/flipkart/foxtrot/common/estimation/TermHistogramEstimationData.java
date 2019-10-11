package com.flipkart.foxtrot.common.estimation;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

/**
 * Estimated cardinality data
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TermHistogramEstimationData extends EstimationData {

    private static final long serialVersionUID = 1596880249082449077L;

    private Map<String, Long> termCounts;

    public TermHistogramEstimationData() {
        super(EstimationDataType.TERM_HISTOGRAM);
    }

    @Builder
    public TermHistogramEstimationData(long count, Map<String, Long> termCounts) {
        super(EstimationDataType.TERM_HISTOGRAM, count);
        this.termCounts = termCounts;
    }

    @Override
    public <T> T accept(EstimationDataVisitor<T> estimationDataVisitor) {
        return estimationDataVisitor.visit(this);
    }
}
