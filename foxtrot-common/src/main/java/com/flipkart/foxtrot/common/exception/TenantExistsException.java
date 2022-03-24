package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class TenantExistsException extends FoxtrotException {

    private final String tenantName;

    protected TenantExistsException(String tenantName) {
        super(ErrorCode.TENANT_ALREADY_EXISTS);
        this.tenantName = tenantName;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("tenant", this.tenantName);
        return map;
    }
}
