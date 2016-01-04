package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
public class TableInitializationException extends FoxtrotException {

    private String table;
    private String reason;

    protected TableInitializationException(String table, String reason) {
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

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        map.put("message", reason);
        return map;
    }
}
