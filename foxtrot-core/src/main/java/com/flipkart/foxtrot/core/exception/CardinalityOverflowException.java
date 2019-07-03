package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Thrown when a group by query fails cardinality check
 */
@Getter
public class CardinalityOverflowException extends FoxtrotException {

    private static final long serialVersionUID = -8591567152701424689L;

    private final String field;
    private final ActionRequest actionRequest;
    private final double probability;

    protected CardinalityOverflowException(ActionRequest actionRequest, String field, double probability) {
        super(ErrorCode.CARDINALITY_OVERFLOW,
                "Query blocked due to high cardinality. Consider using shorter time period");
        this.field = field;
        this.actionRequest = actionRequest;
        this.probability = probability;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("field", this.field);
        map.put("probability", this.probability);
        map.put("request", this.actionRequest);
        return map;
    }
}
