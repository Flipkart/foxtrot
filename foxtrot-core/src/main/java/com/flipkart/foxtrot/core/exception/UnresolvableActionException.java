package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class UnresolvableActionException extends FoxtrotException {

    private final ActionRequest actionRequest;

    protected UnresolvableActionException(ActionRequest actionRequest) {
        super(ErrorCode.UNRESOLVABLE_OPERATION);
        this.actionRequest = actionRequest;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("request", this.actionRequest);
        return map;
    }
}
