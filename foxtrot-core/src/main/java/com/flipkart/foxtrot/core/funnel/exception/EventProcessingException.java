package com.flipkart.foxtrot.core.funnel.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;

/***
 Created by nitish.goyal on 25/09/18
 ***/
public class EventProcessingException extends FoxtrotException {

    public EventProcessingException(String message) {
        super(ErrorCode.EXECUTION_EXCEPTION, message);
    }

    public EventProcessingException(String message, Throwable cause) {
        super(ErrorCode.EXECUTION_EXCEPTION, message, cause);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.getCause()
                .getMessage());
        return map;
    }
}
