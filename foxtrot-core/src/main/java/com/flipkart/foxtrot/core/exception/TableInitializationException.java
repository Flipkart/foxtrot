package com.flipkart.foxtrot.core.exception;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
public class TableInitializationException extends FoxtrotException {

    private String table;
    private String reason;

    public TableInitializationException(String table, String reason) {
        super(ErrorCode.TABLE_INITIALIZATION_ERROR);
        this.table = table;
        this.reason = reason;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
