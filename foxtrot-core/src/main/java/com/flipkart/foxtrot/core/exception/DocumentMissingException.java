package com.flipkart.foxtrot.core.exception;

import java.util.List;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
public class DocumentMissingException extends FoxtrotException {

    private String table;
    private List<String> ids;

    public DocumentMissingException(String table, List<String> ids) {
        super(ErrorCode.DOCUMENT_NOT_FOUND);
        this.table = table;
        this.ids = ids;
    }

    public DocumentMissingException(String table, List<String> ids, Throwable cause) {
        super(ErrorCode.DOCUMENT_NOT_FOUND, cause);
        this.table = table;
        this.ids = ids;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    @Override
    public String toString() {
        return "DocumentMissingException{" +
                "table='" + table + '\'' +
                ", ids=" + ids +
                '}';
    }
}
