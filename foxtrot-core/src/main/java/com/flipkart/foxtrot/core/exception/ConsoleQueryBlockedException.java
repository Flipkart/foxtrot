package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by nitish.goyal on 10/09/19
 ***/
public class ConsoleQueryBlockedException extends FoxtrotException {

    private static final long serialVersionUID = -8591567152701424684L;

    private final ActionRequest actionRequest;

    public ConsoleQueryBlockedException(ActionRequest actionRequest) {
        super(ErrorCode.CONSOLE_QUERY_BLOCKED,
              "Console Query blocked due to high load. Kindly run after sometime");
        this.actionRequest = actionRequest;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("request", this.actionRequest);
        return map;
    }

}
