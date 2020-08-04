package com.flipkart.foxtrot.common.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;

/**
 * Thrown when a group by query fails cardinality check
 */
@Getter
public class CardinalityOverflowException extends FoxtrotException {

    private static final long serialVersionUID = -8591567152701424689L;

    private final String requestStr;
    private final String consoleId;
    private final String field;
    private final ActionRequest actionRequest;
    private final double probability;

    public CardinalityOverflowException(ActionRequest actionRequest,
                                        String requestStr,
                                        String field,
                                        String consoleId,
                                        double probability) {
        super(ErrorCode.CARDINALITY_OVERFLOW,
                "Query blocked due to high cardinality. Consider using shorter time period");
        this.requestStr = requestStr;
        this.field = field;
        this.consoleId = consoleId;
        this.actionRequest = actionRequest;
        this.probability = probability;
    }

    @Override
    public Map<String, Object> toMap() {
        return ImmutableMap.<String, Object>builder().put("field", this.field)
                .put("probability", this.probability)
                .put("consoleId", this.consoleId)
                .put("request", this.actionRequest)
                .put("requestStr", this.requestStr)
                .build();
    }
}