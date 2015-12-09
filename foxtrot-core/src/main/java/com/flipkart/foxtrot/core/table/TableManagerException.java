package com.flipkart.foxtrot.core.table;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
public class TableManagerException extends Exception {

    public enum ErrorCode {
        BAD_REQUEST,
        TABLE_NOT_FOUND,
        TABLE_ALREADY_EXISTS,
        INTERNAL_ERROR
    }

    private final ErrorCode errorCode;

    public TableManagerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public TableManagerException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public TableManagerException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("errorCode", errorCode)
                .toString();
    }

}
