package com.flipkart.foxtrot.gandalf.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

public class UserNotFoundException extends FoxtrotException {
    private final String message;

    public UserNotFoundException(String message) {
        super(ErrorCode.USER_NOT_FOUND);
        this.message = message;
    }

    public UserNotFoundException(String message, Throwable event) {
        super(ErrorCode.USER_NOT_FOUND, event);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
