package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class MalformedQueryException extends FoxtrotException {

    private final ActionRequest actionRequest;
    private final List<String> reasons;

    protected MalformedQueryException(ActionRequest actionRequest, List<String> reasons) {
        super(ErrorCode.MALFORMED_QUERY);
        this.actionRequest = actionRequest;
        this.reasons = reasons;
    }

    protected MalformedQueryException(ErrorCode errorCode, ActionRequest actionRequest, List<String> reasons) {
        super(errorCode);
        this.actionRequest = actionRequest;
        this.reasons = reasons;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("request", this.actionRequest);
        map.put("messages", reasons);
        return map;
    }
}
