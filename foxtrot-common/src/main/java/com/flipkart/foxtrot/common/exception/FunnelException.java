package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by nitish.goyal on 21/05/20
 ***/
public class FunnelException extends FoxtrotException {

    private final String message;

    public FunnelException(String message,
                           Throwable cause) {
        super(ErrorCode.FUNNEL_EXCEPTION, cause);
        this.message = message;
    }

    public FunnelException(String message) {
        super(ErrorCode.FUNNEL_EXCEPTION);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }


}
