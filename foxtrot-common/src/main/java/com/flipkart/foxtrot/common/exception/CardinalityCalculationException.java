package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Map;

/**
 * Thrown when a group by query fails cardinality check
 */
@Getter
public class CardinalityCalculationException extends FoxtrotException {

    private static final long serialVersionUID = -8591567152701424689L;

    private final String message;

    public CardinalityCalculationException(String message) {
        super(ErrorCode.CARDINALITY_CALCULATION_FAILURE, "cardinality estimation failure");
        this.message = message;
    }

    public CardinalityCalculationException(String message,
                                           Throwable cause) {
        super(ErrorCode.CARDINALITY_CALCULATION_FAILURE, "cardinality estimation failure", cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        return ImmutableMap.<String, Object>builder().put("message", this.message)
                .put("errorCode", getCode())
                .put("cause", getCause())
                .put("causeMessage", getCause() != null
                        ? getCause().getMessage()
                        : "")
                .build();
    }
}
