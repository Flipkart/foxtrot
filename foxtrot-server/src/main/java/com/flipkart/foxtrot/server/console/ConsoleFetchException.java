package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class ConsoleFetchException extends FoxtrotException {

    public ConsoleFetchException(Throwable cause) {
        super(ErrorCode.CONSOLE_FETCH_EXCEPTION, cause);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.getCause().getMessage());
        return map;
    }
}
