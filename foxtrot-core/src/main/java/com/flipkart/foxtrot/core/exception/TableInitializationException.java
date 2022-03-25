package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
@Getter
public class TableInitializationException extends FoxtrotException {

    private final String table;
    private final String reason;

    protected TableInitializationException(String table, String reason) {
        super(ErrorCode.TABLE_INITIALIZATION_ERROR);
        this.table = table;
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
