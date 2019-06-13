package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class StoreConnectionException extends FoxtrotException {

    private final String table;

    protected StoreConnectionException(String table, Throwable cause) {
        super(ErrorCode.STORE_CONNECTION_ERROR, cause);
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

