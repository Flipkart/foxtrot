package com.flipkart.foxtrot.core.exception;

import lombok.Getter;

import java.util.Map;

/**
 * Created by rishabh.goyal on 13/12/15.
 */
@Getter
public abstract class FoxtrotException extends RuntimeException {

    private final ErrorCode code;

    protected FoxtrotException(ErrorCode code) {
        this.code = code;
    }

    protected FoxtrotException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    protected FoxtrotException(ErrorCode code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    protected FoxtrotException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public abstract Map<String, Object> toMap();

}
