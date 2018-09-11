package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;

/**
 * Thrown when a group by query fails cardinality check
 */
@Getter
public class CardinalityOverflowException extends MalformedQueryException {

    private static final long serialVersionUID = -8591567152701424689L;

    private String field;
    private double probability;

    protected CardinalityOverflowException(ActionRequest actionRequest, String field, double probability) {
        super(ErrorCode.CARDINALITY_OVERFLOW,
                actionRequest,
                Collections.singletonList("Query blocked as probability > 0.5"));
        this.field = field;
        this.probability = probability;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("request", super.getActionRequest());
        map.put("field", field);
        map.put("probability", probability);
        return map;
    }
}
