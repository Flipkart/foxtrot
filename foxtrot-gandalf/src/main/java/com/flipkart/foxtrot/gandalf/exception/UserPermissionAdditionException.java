package com.flipkart.foxtrot.gandalf.exception;

import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

public class UserPermissionAdditionException extends FoxtrotException {
    private final String message;

    public UserPermissionAdditionException(String message) {
        super(ErrorCode.USER_PERMISSION_ADDITION_FAILURE);
        this.message = message;
    }

    public UserPermissionAdditionException(String message, Throwable event) {
        super(ErrorCode.USER_PERMISSION_ADDITION_FAILURE, event);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
