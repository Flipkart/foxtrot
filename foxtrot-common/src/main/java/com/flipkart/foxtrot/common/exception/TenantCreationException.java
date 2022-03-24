package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class TenantCreationException extends FoxtrotException {

    private final String reason;

    protected TenantCreationException(String reason) {
        super(ErrorCode.TENANT_NOT_CREATED);
        this.reason = reason;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", reason);
        return map;
    }
}
