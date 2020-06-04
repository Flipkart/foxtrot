package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class DataCleanupException extends FoxtrotException {

    private final String message;

    protected DataCleanupException(String message,
                                   Throwable cause) {
        super(ErrorCode.DATA_CLEANUP_ERROR, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
