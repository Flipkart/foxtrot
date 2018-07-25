package com.flipkart.foxtrot.common.estimation;

/**
 * Handle {@link EstimationData} sub-classes.
 */
public interface EstimationDataVisitor<T> {

    T visit(FixedEstimationData fixedEstimationData);

    T visit(PercentileEstimationData percentileEstimationData);

    T visit(CardinalityEstimationData cardinalityEstimationData);

    T visit(TermHistogramEstimationData termHistogramEstimationData);
}
