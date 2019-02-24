package com.flipkart.foxtrot.core.exception;

import java.util.Map;

/**
 * Created by rishabh.goyal on 13/12/15.
 */
public abstract class FoxtrotException extends Exception {

    private ErrorCode code;

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

    public ErrorCode getCode() {
        return code;
    }

    public void setCode(ErrorCode code) {
        this.code = code;
    }


}
