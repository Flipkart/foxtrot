package com.flipkart.foxtrot.core.exception;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class TableMissingException extends FoxtrotException {

    private String table;

    public TableMissingException(String table) {
        super(ErrorCode.TABLE_NOT_FOUND);
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}

