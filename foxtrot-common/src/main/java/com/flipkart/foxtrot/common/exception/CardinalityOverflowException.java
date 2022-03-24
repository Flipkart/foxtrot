package com.flipkart.foxtrot.common.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.cardinality.ProbabilityCalculationResult;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Thrown when a group by query fails cardinality check
 */
@Getter
public class CardinalityOverflowException extends FoxtrotException {

    private static final long serialVersionUID = -8591567152701424689L;

    private final String requestStr;
    private final String consoleId;
    private final List<String> fields;
    private final ActionRequest actionRequest;
    private final String cacheKey;
    private final ProbabilityCalculationResult probability;

    public CardinalityOverflowException(ActionRequest actionRequest,
                                        String requestStr,
                                        List<String> fields,
                                        String consoleId,
                                        String cacheKey,
                                        ProbabilityCalculationResult probability) {
        super(ErrorCode.CARDINALITY_OVERFLOW,
                "Query blocked due to high cardinality. Consider using shorter time period");
        this.requestStr = requestStr;
        this.fields = fields;
        this.consoleId = consoleId;
        this.actionRequest = actionRequest;
        this.cacheKey = cacheKey;
        this.probability = probability;
    }

    @Override
    public Map<String, Object> toMap() {
        return ImmutableMap.<String, Object>builder().put("fields", this.fields.toString())
                .put("probability", this.probability.getProbability())
                .put("probabilityCalculation", JsonUtils.toJson(probability))
                .put("consoleId", this.consoleId)
                .put("request", this.actionRequest)
                .put("cacheKey", this.cacheKey)
                .put("requestStr", this.requestStr)
                .build();
    }
}
