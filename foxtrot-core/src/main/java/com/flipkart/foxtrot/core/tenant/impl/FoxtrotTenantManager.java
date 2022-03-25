package com.flipkart.foxtrot.core.tenant.impl;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.exception.BadRequestException;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.tenant.TenantManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.flipkart.foxtrot.common.exception.FoxtrotExceptions.ERROR_DELIMITER;

public class FoxtrotTenantManager implements TenantManager {

    private static Pattern VALID_TENANT_NAME_REGEX = Pattern.compile("^[A-Z-]+$");
    private final TenantMetadataManager metadataManager;
    private final QueryStore queryStore;

    @Inject
    public FoxtrotTenantManager(TenantMetadataManager metadataManager,
                                QueryStore queryStore) {
        this.metadataManager = metadataManager;
        this.queryStore = queryStore;
    }

    @Override
    public void save(Tenant tenant) {
        validateTenantParams(tenant);
        if (metadataManager.exists(tenant.getTenantName())) {
            throw FoxtrotExceptions.createTenantExistsException(tenant.getTenantName());
        }
        queryStore.initializeTenant(tenant.getTenantName());
        metadataManager.save(tenant);
    }


    @Override
    public void saveAll(List<Tenant> tenants) {

        List<String> exceptionMessages = new ArrayList<>();
        BadRequestException badRequestException = null;
        validateTenantParams(tenants);
        for (Tenant tenant : tenants) {
            try {
                save(tenant);
            } catch (BadRequestException e) {
                badRequestException = e;
            } catch (Exception e) {
                exceptionMessages.add(Objects.nonNull(e.getCause())
                        ? e.getCause()
                        .getMessage()
                        : e.getMessage());
            }
        }

        if (!exceptionMessages.isEmpty()) {
            String exceptionMessage = String.join(ERROR_DELIMITER, exceptionMessages);
            throw FoxtrotExceptions.createTenantCreationException(exceptionMessage);
        } else if (Objects.nonNull(badRequestException)) {
            throw badRequestException;
        }
    }

    @Override
    public Tenant get(String tenantName) {
        Tenant tenant = metadataManager.get(tenantName);
        if (tenant == null) {

            throw FoxtrotExceptions.createTenantMissingException(tenantName);
        }
        return tenant;
    }

    @Override
    public List<Tenant> getAll() {
        return metadataManager.get();
    }

    @Override
    public void update(Tenant tenant) {
        validateTenantParams(tenant);
        if (!metadataManager.exists(tenant.getTenantName())) {
            throw FoxtrotExceptions.createTenantMissingException(tenant.getTenantName());
        }
        metadataManager.save(tenant);
    }

    private void validateTenantParams(Tenant tenant) {
        if (tenant == null || tenant.getTenantName() == null || tenant.getTenantName()
                .trim()
                .isEmpty() || tenant.getEmailIds().length <= 0) {
            throw FoxtrotExceptions.createBadRequestException(tenant != null
                    ? tenant.getTenantName()
                    : null, "Invalid Tenant Params");
        } else {

            boolean isValidTenant = VALID_TENANT_NAME_REGEX.matcher(tenant.getTenantName())
                    .matches();
            if (!isValidTenant) {
                throw FoxtrotExceptions.createBadRequestException(tenant.getTenantName(),
                        "Tenant Name should contain Upper case and hyphen only");
            }
        }

        tenant.setEmailIds(Arrays.stream(tenant.getEmailIds())
                .distinct()
                .toArray(String[]::new));
    }

    private void validateTenantParams(List<Tenant> tenants) {
        if (CollectionUtils.isEmpty(tenants)) {
            throw FoxtrotExceptions.createBadRequestException(null, "Empty Tenant list");
        }
    }
}
