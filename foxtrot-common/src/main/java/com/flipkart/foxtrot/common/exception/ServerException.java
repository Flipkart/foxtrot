package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class ServerException extends FoxtrotException {

    private final String message;

    protected ServerException(String message, Throwable cause) {
        super(ErrorCode.EXECUTION_EXCEPTION, cause);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
