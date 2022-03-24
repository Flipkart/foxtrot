package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

/***
 Created by nitish.goyal on 10/09/19
 ***/
public class FqlQueryBlockedException extends FoxtrotException {

    private static final long serialVersionUID = 6153780592761103256L;

    private final String query;

    protected FqlQueryBlockedException(String query) {
        super(ErrorCode.FQL_QUERY_BLOCKED, "FQL Query blocked due to high load. Kindly run after sometime");
        this.query = query;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("query", this.query);
        return map;
    }

}
