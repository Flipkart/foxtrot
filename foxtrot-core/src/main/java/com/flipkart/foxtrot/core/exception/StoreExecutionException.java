package com.flipkart.foxtrot.core.exception;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class StoreExecutionException extends FoxtrotException {

    private String table;

    public StoreExecutionException(String table, Throwable cause) {
        super(ErrorCode.STORE_EXECUTION_ERROR, cause);
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
