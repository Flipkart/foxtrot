package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SerDeException extends FoxtrotException {

    public SerDeException(ErrorCode code) {
        super(code);
    }

    public SerDeException(ErrorCode code,
                          String message) {
        super(code, message);
    }

    public SerDeException(ErrorCode code,
                          Throwable cause) {
        super(code, cause);
    }

    public SerDeException(ErrorCode code,
                          String message,
                          Throwable cause) {
        super(code, message, cause);
    }

    @Override
    public Map<String, Object> toMap() {
        return ImmutableMap.of("code", getCode().name(), "message", this.getCause() != null
                ? this.getCause()
                .getMessage()
                : this.getMessage());
    }
}
