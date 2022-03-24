package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

public class ElasticsearchQueryStoreException extends FoxtrotException {

    private final String message;

    public ElasticsearchQueryStoreException(String message,
                                            Throwable cause) {
        super(ErrorCode.ELASTICSEARCH_QUERY_STORE_EXCEPTION, cause);
        this.message = message;
    }

    public ElasticsearchQueryStoreException(String message) {
        super(ErrorCode.ELASTICSEARCH_QUERY_STORE_EXCEPTION);
        this.message = message;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }


}
