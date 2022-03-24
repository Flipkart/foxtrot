package com.flipkart.foxtrot.common.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class ActionResolutionException extends FoxtrotException {

    private final ActionRequest actionRequest;

    protected ActionResolutionException(ActionRequest actionRequest,
                                        Throwable cause) {
        super(ErrorCode.ACTION_RESOLUTION_FAILURE, cause);
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
