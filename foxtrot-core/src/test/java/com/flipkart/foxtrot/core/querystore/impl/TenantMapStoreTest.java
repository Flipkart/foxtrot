package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.tenant.impl.TenantMapStore;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;


public class TenantMapStoreTest {

    public static final String TEST_TENANT = "test-tenant";
    public static final String TEST_EMAIL = "testemail@gmail.com";
    public static final String TENANT_META_INDEX = "tenant-meta";
    public static final String TENANT_META_TYPE = "tenant-meta";
    private static ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper mapper = new ObjectMapper();
    private TenantMapStore tenantMapStore;


    @BeforeClass
    public static void setupClass() throws Exception {
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        elasticsearchConnection.stop();
    }

    @Before
    public void setUp() throws Exception {
        TestUtils.ensureIndex(elasticsearchConnection, TenantMapStore.TENANT_META_INDEX);
        TenantMapStore.Factory factory = new TenantMapStore.Factory(elasticsearchConnection);
        tenantMapStore = factory.newMapStore(null, null);
    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }

    @Test
    public void testStore() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setTenantName(TEST_TENANT);
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenantMapStore.store(tenant.getTenantName(), tenant);

        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, tenant.getTenantName()),
                        RequestOptions.DEFAULT);
        compareTenants(tenant, mapper.readValue(response.getSourceAsBytes(), Tenant.class));
    }

    private void compareTenants(Tenant expected,
                                Tenant actual) {
        assertNotNull(actual);
        assertEquals(expected.getTenantName(), actual.getTenantName());
        assertTrue(CollectionUtils.isEqualCollection(new ArrayList<>(Arrays.asList(TEST_EMAIL)),
                Arrays.asList(actual.getEmailIds())));
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullKey() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setTenantName(TEST_TENANT);
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenantMapStore.store(null, tenant);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullTenant() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setTenantName(TEST_TENANT);
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenantMapStore.store(tenant.getTenantName(), null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullTenantName() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setTenantName(null);
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenantMapStore.store(TEST_TENANT, tenant);
    }

    @Test
    public void testStoreAll() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
        }
        tenantMapStore.storeAll(tenants);
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        tenants.keySet()
                .forEach(key -> multiGetRequest.add(TENANT_META_INDEX, TENANT_META_TYPE, key));
        MultiGetResponse response = elasticsearchConnection.getClient()
                .mget(multiGetRequest, RequestOptions.DEFAULT);
        Map<String, Tenant> responseTenants = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            Tenant tenant = mapper.readValue(multiGetItemResponse.getResponse()
                    .getSourceAsString(), Tenant.class);
            responseTenants.put(tenant.getTenantName(), tenant);
        }
        for (Map.Entry<String, Tenant> entry : tenants.entrySet()) {
            compareTenants(entry.getValue(), responseTenants.get(entry.getKey()));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNull() throws Exception {
        tenantMapStore.storeAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTenantKey() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(null, tenant);
        }
        tenantMapStore.storeAll(tenants);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTenantValue() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            tenants.put(UUID.randomUUID()
                    .toString(), null);
        }
        tenantMapStore.storeAll(tenants);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTenantKeyValue() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            tenants.put(null, null);
        }
        tenantMapStore.storeAll(tenants);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeys() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
        }

        Tenant tenant = new Tenant();
        tenant.setTenantName(UUID.randomUUID()
                .toString());
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenants.put(null, tenant);
        tenantMapStore.storeAll(tenants);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullValues() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
        }

        Tenant tenant = new Tenant();
        tenant.setTenantName(UUID.randomUUID()
                .toString());
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenants.put(tenant.getTenantName(), null);
        tenantMapStore.storeAll(tenants);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeyValues() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
        }
        tenants.put(null, null);
        tenantMapStore.storeAll(tenants);
    }

    @Test
    public void testDelete() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setTenantName(TEST_TENANT);
        tenant.setEmailIds(new String[]{TEST_EMAIL});
        tenantMapStore.store(tenant.getTenantName(), tenant);
        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, tenant.getTenantName()),
                        RequestOptions.DEFAULT);
        assertTrue(response.isExists());

        tenantMapStore.delete(tenant.getTenantName());
        response = elasticsearchConnection.getClient()
                .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, tenant.getTenantName()),
                        RequestOptions.DEFAULT);
        assertFalse(response.isExists());
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteNullKey() throws Exception {
        tenantMapStore.delete(null);
    }

    @Test
    public void testDeleteAll() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
        }
        tenantMapStore.storeAll(tenants);
        for (String name : tenants.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertTrue(response.isExists());
        }

        tenantMapStore.deleteAll(tenants.keySet());
        for (String name : tenants.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertFalse(response.isExists());
        }

    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNull() throws Exception {
        tenantMapStore.deleteAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNullKeys() throws Exception {
        List<String> keys = new ArrayList<String>();
        keys.add(null);
        keys.add(null);
        tenantMapStore.deleteAll(keys);
    }

    @Test
    public void testLoadAll() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, tenant);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(TENANT_META_INDEX).type(TENANT_META_TYPE)
                            .source(sourceMap)
                            .id(tenant.getTenantName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> names = ImmutableSet.copyOf(Iterables.limit(tenants.keySet(), 5));
        Map<String, Tenant> responseTenants = tenantMapStore.loadAll(names);
        assertEquals(names.size(), responseTenants.size());
        for (String name : names) {
            compareTenants(tenants.get(name), responseTenants.get(name));
        }
    }

    @Test(expected = NullPointerException.class)
    public void testLoadAllNull() throws Exception {
        tenantMapStore.loadAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadAllKeyWithWrongJson() throws Exception {
        elasticsearchConnection.getClient()
                .index(new IndexRequest(TENANT_META_INDEX).type(TENANT_META_TYPE)
                        .source("{ \"test\" : \"test\"}")
                        .id(TEST_TENANT)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        tenantMapStore.loadAll(Arrays.asList(TEST_TENANT));
    }

    @Test
    public void testLoadAllKeys() throws Exception {
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Tenant tenant = new Tenant();
            tenant.setTenantName(UUID.randomUUID()
                    .toString());
            tenant.setEmailIds(new String[]{TEST_EMAIL});
            tenants.put(tenant.getTenantName(), tenant);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, tenant);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(TENANT_META_INDEX).type(TENANT_META_TYPE)
                            .source(sourceMap)
                            .id(tenant.getTenantName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> responseKeys = tenantMapStore.loadAllKeys();
        for (String name : tenants.keySet()) {
            assertTrue(responseKeys.contains(name));
        }
    }
}
