package com.flipkart.foxtrot.gandalf.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

public class EndpointNotFoundException extends FoxtrotException {

    private final String message;

    public EndpointNotFoundException(String message) {
        super(ErrorCode.ENDPOINT_NOT_FOUND_ERROR);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
