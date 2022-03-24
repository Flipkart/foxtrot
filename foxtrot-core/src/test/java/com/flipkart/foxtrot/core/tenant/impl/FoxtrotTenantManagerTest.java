package com.flipkart.foxtrot.core.tenant.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.tenant.TenantManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class FoxtrotTenantManagerTest {

    private TenantManager tenantManager;
    private TenantMetadataManager metadataManager;
    private QueryStore queryStore;

    @Before
    public void setUp() throws Exception {
        this.queryStore = mock(QueryStore.class);
        this.metadataManager = mock(TenantMetadataManager.class);
        this.tenantManager = new FoxtrotTenantManager(metadataManager, queryStore);
    }

    @Test
    public void saveTenantNullName() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName(null);
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.save(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void saveTenantEmptyName() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName(" ");
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.save(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void saveNullTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        try {
            tenantManager.save(null);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void saveTenantEmptyEmailIds() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName("tenant");
            tenant.setEmailIds(new String[]{});
            tenantManager.save(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void saveTenantInvalidtenantName() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName("tenent");
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.save(tenant);
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void saveTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        Tenant tenant = new Tenant();
        tenant.setTenantName("TENANT");
        tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
        tenantManager.save(tenant);
        Assert.assertTrue(true);
    }

    @Test
    public void saveExistingTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        doReturn(true).when(metadataManager)
                .exists(any(String.class));
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName("TENANT");
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.save(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TENANT_ALREADY_EXISTS, e.getCode());
        }
    }

    @Test
    public void updateTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(true).when(metadataManager)
                .exists(anyString());
        Tenant tenant = new Tenant();
        tenant.setTenantName("TENANT");
        tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
        tenantManager.update(tenant);
        Assert.assertTrue(true);
    }

    @Test
    public void updateNonExistingTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());

        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName("TENANT");
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.update(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TENANT_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void updateNullTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());
        try {
            tenantManager.update(null);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void updateTenantNullName() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName(null);
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.update(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void updateTenantEmptyName() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName(" ");
            tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
            tenantManager.update(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void updateTenantEmptyEmailIds() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());
        try {
            Tenant tenant = new Tenant();
            tenant.setTenantName("tenant");
            tenant.setEmailIds(new String[]{});
            tenantManager.update(tenant);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void getTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        Tenant tenant = new Tenant();
        tenant.setTenantName("tenant");
        tenant.setEmailIds(new String[]{"testEmailId@gmail.com"});
        doReturn(tenant).when(metadataManager)
                .get("tenant");
        Tenant getTenant = tenantManager.get("tenant");
        assertEquals("tenant", getTenant.getTenantName());
        assertEquals(1, getTenant.getEmailIds().length);
        assertTrue(CollectionUtils.isEqualCollection(new ArrayList<>(Arrays.asList("testEmailId@gmail.com")),
                Arrays.asList(getTenant.getEmailIds())));
    }

    @Test
    public void getNullTenant() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Tenant.class));
        doReturn(null).when(metadataManager)
                .get("tenant");
        try {
            Tenant getTenant = tenantManager.get("tenant");
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TENANT_NOT_FOUND, e.getCode());
        }
    }
}
