package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.tenant.TenantManager;
import com.flipkart.foxtrot.core.tenant.impl.FoxtrotTenantManager;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class TenantResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    private TenantManager tenantManager;

    public TenantResourceTest() throws Exception {
        super();
        this.tenantManager = new FoxtrotTenantManager(getTenantMetadataManager(), getQueryStore());
        this.tenantManager = spy(tenantManager);
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new TenantResource(tenantManager))
                .build();
    }


    @Test
    public void testSaveNullTenant() throws Exception {
        Response response = resources.target("/v1/tenant")
                .request()
                .post(null);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveNullTenantName() throws Exception {

        Tenant tenant = Tenant.builder()
                .tenantName(null)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        Entity<Tenant> tenantEntity = Entity.json(tenant);
        Response response = resources.target("/v1/tenant")
                .request()
                .post(tenantEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        Entity<Tenant> tenantEntity = Entity.json(tenant);
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(tenantManager)
                .save(Matchers.<Tenant>any());
        Response response = resources.target("/v1/tenant")
                .request()
                .post(tenantEntity);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        reset(tenantManager);
    }

    @Test
    public void testSaveEmptyEmailIds() throws Exception {
        reset(tenantManager);
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{})
                .build();
        Entity<Tenant> tenantEntity = Entity.json(tenant);
        Response response = resources.target("/v1/tenant")
                .request()
                .post(tenantEntity);
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    }

    @Test
    public void testSaveHappyCase() throws Exception {
        reset(tenantManager);
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME + "-A")
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        Entity<Tenant> tenantEntity = Entity.json(tenant);
        Response response = resources.target("/v1/tenant")
                .request()
                .post(tenantEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }

    @Test
    public void testSaveTenantsHappyCase() throws Exception {
        List<Tenant> tenants = new ArrayList<Tenant>();
        Tenant tenant1 = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME + "-A")
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        Tenant tenant2 = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME + "-B")
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        tenants.add(tenant1);
        tenants.add(tenant2);
        Entity<List<Tenant>> listEntity = Entity.json(tenants);
        resources.target("/v1/tenant/bulk")
                .request()
                .post(listEntity);
        compare(tenant1, getTenantMetadataManager().get(TestUtils.TEST_TENANT_NAME + "-A"));
        compare(tenant2, getTenantMetadataManager().get(TestUtils.TEST_TENANT_NAME + "-B"));
    }

    private void compare(Tenant expected,
                         Tenant actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual Tenant name should not be null", actual.getTenantName());
        assertNotNull("Actual tenant emailIds should not be null", actual.getEmailIds());
        assertEquals("Actual Tenant name should match expected Tenant name", expected.getTenantName(),
                actual.getTenantName());
        assertEquals("Actual tenant emailIds should match expected tenant emailIDs", expected.getEmailIds(),
                actual.getEmailIds());
    }

    @Test
    public void testSaveTenantsNullTenants() throws Exception {
        Response response = resources.target("/v1/tenant/bulk")
                .request()
                .post(null);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveTenantsNullTenant() throws Exception {
        List<Tenant> tenants = new Vector<Tenant>();
        tenants.add(null);
        tenants.add(Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME + "-A")
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build());
        Entity<List<Tenant>> listEntity = Entity.json(tenants);
        Response response = resources.target("/v1/tenant/bulk")
                .request()
                .post(listEntity);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveTenantsInvalidRequestObject() throws Exception {
        Response response = resources.target("/v1/tenant/bulk")
                .request()
                .post(Entity.text("foxtrot"));
        assertEquals(Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveTenantsEmptyList() throws Exception {
        Entity<List<Tenant>> list = Entity.json(Collections.emptyList());
        Response response = resources.target("/v1/tenant/bulk")
                .request()
                .post(list);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetTenant() throws Exception {
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        Tenant response = resources.target(String.format("/v1/tenant/%s", TestUtils.TEST_TENANT_NAME))
                .request()
                .get(Tenant.class);
        compare(tenant, response);
    }

    @Test
    public void testUpdateTenant() throws Exception {
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        tenant.setEmailIds(new String[]{"test@email.com"});
        Entity<Tenant> tenantEntity = Entity.json(tenant);
        Response response = resources.target(String.format("/v1/tenant/%s", TestUtils.TEST_TENANT_NAME))
                .request()
                .put(tenantEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }
}
