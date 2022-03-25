package com.flipkart.foxtrot.core.tenant;

import com.flipkart.foxtrot.common.tenant.Tenant;

import java.util.List;

public interface TenantManager {

    void save(Tenant tenant);

    void saveAll(List<Tenant> tenants);

    Tenant get(String tenantName);

    List<Tenant> getAll();

    void update(Tenant tenant);
}
