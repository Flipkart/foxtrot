package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class TenantMissingException extends FoxtrotException {

    private final String tenant;

    protected TenantMissingException(String tenant) {
        super(ErrorCode.TENANT_NOT_FOUND);
        this.tenant = tenant;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("tenant", this.tenant);
        return map;
    }
}