package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class TableMissingException extends FoxtrotException {

    private String table;

    protected TableMissingException(String table) {
        super(ErrorCode.TABLE_NOT_FOUND);
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        return map;
    }
}

