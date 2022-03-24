package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationServiceImpl;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.pipeline.impl.DistributedPipelineMetadataManager;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.tenant.impl.DistributedTenantMetadataManager;
import com.flipkart.foxtrot.pipeline.PipelineExecutor;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.val;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class DistributedTenantMetadataManagerTest {

    private static HazelcastInstance hazelcastInstance;
    private static ElasticsearchConnection elasticsearchConnection;

    private DataStore dataStore;
    private ElasticsearchQueryStore queryStore;
    private DistributedTableMetadataManager distributedTableMetadataManager;
    private DistributedTenantMetadataManager distributedTenantMetadataManager;
    private IMap<String, Tenant> tenantDataStore;
    private ObjectMapper objectMapper;

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        elasticsearchConnection.stop();
    }

    @Before
    public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerSubtypes(GroupResponse.class);
        SerDe.init(objectMapper);
        this.dataStore = Mockito.mock(DataStore.class);

        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        hazelcastConnection.start();
        CardinalityConfig cardinalityConfig = new CardinalityConfig();
        this.distributedTableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                elasticsearchConnection,
                new CardinalityCalculationServiceImpl(cardinalityConfig, elasticsearchConnection), cardinalityConfig);
        this.distributedTenantMetadataManager = new DistributedTenantMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        distributedTenantMetadataManager.start();
        PipelineUtils.init(objectMapper, ImmutableSet.of("com.flipkart.foxtrot.pipeline"));
        PipelineExecutor pipelineExecutor = new PipelineExecutor(
                new ReflectionBasedProcessorFactory(Guice.createInjector(new PipelineModule())));
        val pipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        pipelineMetadataManager.start();
        tenantDataStore = hazelcastInstance.getMap("tenantmetadatamap");
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(objectMapper,
                TextNodeRemoverConfiguration.builder()
                        .build()));
        this.queryStore = new ElasticsearchQueryStore(distributedTableMetadataManager, distributedTenantMetadataManager,
                elasticsearchConnection, dataStore, mutators, new ElasticsearchTemplateMappingParser(),
                new CardinalityConfig());
    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
        distributedTenantMetadataManager.stop();
    }

    @Test
    public void testGet() throws Exception {

        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        tenantDataStore.put(TestUtils.TEST_TENANT_NAME, tenant);
        Tenant response = distributedTenantMetadataManager.get(tenant.getTenantName());
        assertEquals(tenant.getTenantName(), response.getTenantName());
        assertEquals(tenant.getEmailIds(), response.getEmailIds());
    }

    @Test
    public void testGetMissingTenant() throws Exception {
        Tenant response = distributedTenantMetadataManager.get(TestUtils.TEST_TENANT + "-missing");
        assertNull(response);
    }

    @Test
    public void testExists() throws Exception {
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        distributedTenantMetadataManager.save(tenant);
        assertTrue(distributedTenantMetadataManager.exists(tenant.getTenantName()));
        assertFalse(distributedTenantMetadataManager.exists("DUMMY_TEST_NAME_NON_EXISTENT"));
    }

    @Test
    public void testGetEmailIds() {
        Tenant tenant = Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build();
        distributedTenantMetadataManager.save(tenant);
        List<String> emailIds = distributedTenantMetadataManager.getEmailIds(TestUtils.TEST_TENANT_NAME);
        assertEquals(emailIds, Arrays.asList(tenant.getEmailIds()));
    }
}
