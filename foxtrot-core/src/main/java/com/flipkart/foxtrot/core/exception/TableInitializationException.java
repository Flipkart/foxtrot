package com.flipkart.foxtrot.core.exception;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
public class TableInitializationException extends FoxtrotException {

    private String table;
    private String message;

    public TableInitializationException(String table, String message) {
        super(ErrorCode.TABLE_INITIALIZATION_ERROR);
        this.table = table;
        this.message = message;
    }

    public TableInitializationException(String table, String message, Throwable cause) {
        super(ErrorCode.TABLE_INITIALIZATION_ERROR, cause);
        this.table = table;
        this.message = message;
    }
}
