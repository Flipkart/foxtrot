package com.flipkart.foxtrot.gandalf.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

public class AuthTokenException extends FoxtrotException {

    private final String message;

    public AuthTokenException(String message,
                              Throwable event) {
        super(ErrorCode.AUTH_TOKEN_EXCEPTION, event);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
