package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Map;

/**
 * Thrown when a something goes wrong in FieldCardinalityMapStore
 */
@Getter
public class CardinalityMapStoreException extends FoxtrotException {

    private static final long serialVersionUID = -5763942835369223156L;

    private final String message;

    public CardinalityMapStoreException(String message) {
        super(ErrorCode.CARDINALITY_MAP_STORE_ERROR, "cardinality map store error");
        this.message = message;
    }

    public CardinalityMapStoreException(String message,
                                        Throwable cause) {
        super(ErrorCode.CARDINALITY_MAP_STORE_ERROR, "cardinality map store error", cause);
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
