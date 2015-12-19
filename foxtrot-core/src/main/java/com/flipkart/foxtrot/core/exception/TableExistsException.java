package com.flipkart.foxtrot.core.exception;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class TableExistsException extends FoxtrotException {

    private String table;

    public TableExistsException(String table) {
        super(ErrorCode.TABLE_ALREADY_EXISTS);
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
