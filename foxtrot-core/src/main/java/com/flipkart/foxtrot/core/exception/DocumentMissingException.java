package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
public class DocumentMissingException extends FoxtrotException {

    private String table;
    private List<String> ids;

    protected DocumentMissingException(String table, List<String> ids) {
        super(ErrorCode.DOCUMENT_NOT_FOUND);
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
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        map.put("ids", ids);
        return map;
    }
}
