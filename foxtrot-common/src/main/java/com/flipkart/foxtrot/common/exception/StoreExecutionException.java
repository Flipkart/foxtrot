package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class StoreExecutionException extends FoxtrotException {

    private final String table;

    protected StoreExecutionException(String table, Throwable cause) {
        super(ErrorCode.STORE_EXECUTION_ERROR, cause);
        this.table = table;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        map.put("message", this.getCause()
                .getMessage());
        return map;
    }
}
