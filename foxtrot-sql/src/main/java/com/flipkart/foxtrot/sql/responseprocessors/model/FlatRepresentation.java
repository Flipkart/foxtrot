package com.flipkart.foxtrot.sql.responseprocessors.model;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class FlatRepresentation {
    private String opcode;
    private List<FieldHeader> headers = Lists.newArrayList();
    private List<Map<String, Object>> rows = Lists.newArrayList();

    public FlatRepresentation(List<FieldHeader> headers, List<Map<String, Object>> rows) {
        this.headers = headers;
        this.rows = rows;
    }

    public FlatRepresentation(String opcode, List<FieldHeader> headers, List<Map<String, Object>> rows) {
        this.opcode = opcode;
        this.headers = headers;
        this.rows = rows;
    }

    public String getOpcode() {
        return opcode;
    }

    public void setOpcode(String opcode) {
        this.opcode = opcode;
    }

    public List<FieldHeader> getHeaders() {
        return headers;
    }

    public void setHeaders(List<FieldHeader> headers) {
        this.headers = headers;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }
}
