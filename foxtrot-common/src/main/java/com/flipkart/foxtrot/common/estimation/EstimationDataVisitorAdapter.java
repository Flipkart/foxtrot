package com.flipkart.foxtrot.common.estimation;

/**
 * Adapter to make life simple
 */
public class EstimationDataVisitorAdapter<T> implements EstimationDataVisitor<T> {
    @Override
    public T visit(FixedEstimationData fixedEstimationData) {
        return null;
    }

    @Override
    public T visit(PercentileEstimationData percentileEstimationData) {
        return null;
    }

    @Override
    public T visit(CardinalityEstimationData cardinalityEstimationData) {
        return null;
    }
}
