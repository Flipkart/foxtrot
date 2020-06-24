package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
@Getter
public class TableInitializationException extends FoxtrotException {

    private final String table;
    private final String reason;

    protected TableInitializationException(String table,
                                           String reason) {
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
