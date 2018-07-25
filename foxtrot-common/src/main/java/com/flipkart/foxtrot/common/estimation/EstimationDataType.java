package com.flipkart.foxtrot.common.estimation;

/**
 * Type of estimation data
 */
public enum EstimationDataType {
    /**
     * Fixed estimation data, for:
     *  {@link com.flipkart.foxtrot.common.FieldType#BOOLEAN}
     */
    FIXED,
    /**
     * Percentile based estimation for:
     *  {@link com.flipkart.foxtrot.common.FieldType#LONG}
     *  {@link com.flipkart.foxtrot.common.FieldType#DATE}
     *  {@link com.flipkart.foxtrot.common.FieldType#DOUBLE}
     *  {@link com.flipkart.foxtrot.common.FieldType#FLOAT}
     *  {@link com.flipkart.foxtrot.common.FieldType#INTEGER}
     */
    PERCENTILE,
    /**
     * Fixed estimation data, for:
     *  {@link com.flipkart.foxtrot.common.FieldType#STRING}
     */
    CARDINALITY,

    TERM_HISTOGRAM
}
