package com.flipkart.foxtrot.gandalf.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

public class PermissionCreationException extends FoxtrotException {
    private final String message;

    public PermissionCreationException(String message, Throwable event) {
        super(ErrorCode.PERMISSION_CREATION_FAILURE, event);
        this.message = message;
    }

    public PermissionCreationException(String message) {
        super(ErrorCode.PERMISSION_CREATION_FAILURE);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
