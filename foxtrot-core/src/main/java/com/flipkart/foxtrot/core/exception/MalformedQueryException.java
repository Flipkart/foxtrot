package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class MalformedQueryException extends FoxtrotException {

    private ActionRequest actionRequest;
    private List<String> reasons;

    protected MalformedQueryException(ActionRequest actionRequest, List<String> reasons) {
        super(ErrorCode.MALFORMED_QUERY);
        this.actionRequest = actionRequest;
        this.reasons = reasons;
    }

    public ActionRequest getActionRequest() {
        return actionRequest;
    }

    public void setActionRequest(ActionRequest actionRequest) {
        this.actionRequest = actionRequest;
    }

    public List<String> getReasons() {
        return reasons;
    }

    public void setReasons(List<String> reasons) {
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
