package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class QueryCreationException extends FoxtrotException {

    private final ActionRequest actionRequest;

    protected QueryCreationException(ActionRequest actionRequest, Throwable cause) {
        super(ErrorCode.MALFORMED_QUERY, cause);
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
