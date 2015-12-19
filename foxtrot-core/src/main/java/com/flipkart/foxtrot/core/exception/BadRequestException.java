package com.flipkart.foxtrot.core.exception;

import java.util.Collections;
import java.util.List;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class BadRequestException extends FoxtrotException {

    private String table;
    private List<String> messages;

    public BadRequestException(String table, List<String> messages) {
        super(ErrorCode.INVALID_REQUEST);
        this.table = table;
        this.messages = messages;
    }

    public BadRequestException(String table, Exception e) {
        super(ErrorCode.INVALID_REQUEST, e);
        this.table = table;
        this.messages = Collections.singletonList(e.getMessage());
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
}
