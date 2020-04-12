package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/***
 Created by mudit.g on Mar, 2019
 ***/
@Getter
public class AuthorizationException extends FoxtrotException {

    private final ActionRequest actionRequest;

    protected AuthorizationException(ActionRequest actionRequest, Throwable cause) {
        super(ErrorCode.AUTHORIZATION_EXCEPTION, cause);
        this.actionRequest = actionRequest;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("request", this.actionRequest);
        map.put("message", this.getCause()
                .getMessage());
        return map;
    }
}
