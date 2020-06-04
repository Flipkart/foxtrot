package com.flipkart.foxtrot.core.funnel.exception;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Getter
public class FunnelException extends FoxtrotException {

    public FunnelException(ErrorCode errorCode) {
        super(errorCode);
    }

    public FunnelException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public FunnelException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public FunnelException(ErrorCode errorCode,
                           String message,
                           Throwable cause) {
        super(errorCode, message, cause);
    }


    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.getCause() != null
                           ? this.getCause()
                                   .getMessage()
                           : this.getMessage());
        return map;
    }
}
