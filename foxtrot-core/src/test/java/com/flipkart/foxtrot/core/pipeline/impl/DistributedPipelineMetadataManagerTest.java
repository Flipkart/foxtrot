package com.flipkart.foxtrot.core.pipeline.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationServiceImpl;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.tenant.impl.DistributedTenantMetadataManager;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.PipelineExecutor;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.google.common.collect.ImmutableList;
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

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class DistributedPipelineMetadataManagerTest {

    private static HazelcastInstance hazelcastInstance;
    private static ElasticsearchConnection elasticsearchConnection;

    private DistributedTenantMetadataManager distributedTenantMetadataManager;
    private DistributedTableMetadataManager distributedTableMetadataManager;
    private DistributedPipelineMetadataManager distributedPipelineMetadataManager;
    private IMap<String, Pipeline> pipelineDataStore;
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
        this.distributedPipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        distributedTenantMetadataManager.start();
        distributedPipelineMetadataManager.start();
        PipelineUtils.init(objectMapper, ImmutableSet.of("com.flipkart.foxtrot.pipeline"));
        PipelineExecutor pipelineExecutor = new PipelineExecutor(
                new ReflectionBasedProcessorFactory(Guice.createInjector(new PipelineModule())));
        val pipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        pipelineMetadataManager.start();
        pipelineDataStore = hazelcastInstance.getMap("pipelinemetadatamap");
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(objectMapper,
                TextNodeRemoverConfiguration.builder()
                        .build()));

    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
        distributedTenantMetadataManager.stop();
    }

    @Test
    public void testGet() throws Exception {

        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition("$.matchField")))
                .build();
        pipelineDataStore.put(TestUtils.TEST_PIPELINE_NAME, pipeline);
        Pipeline response = distributedPipelineMetadataManager.get(pipeline.getName());
        assertEquals(pipeline.getName(), response.getName());
        assertEquals(pipeline.getProcessors()
                .size(), response.getProcessors()
                .size());
    }

    @Test
    public void testGetMissingTenant() throws Exception {
        Pipeline response = distributedPipelineMetadataManager.get(TestUtils.TEST_PIPELINE_NAME + "-missing");
        assertNull(response);
    }

    @Test
    public void testExists() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition("$.matchField")))
                .build();
        distributedPipelineMetadataManager.save(pipeline);
        assertTrue(distributedPipelineMetadataManager.exists(pipeline.getName()));
        assertFalse(distributedPipelineMetadataManager.exists("DUMMY_TEST_NAME_NON_EXISTENT"));
    }

    @Test
    public void testGetAll() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition("$.matchField")))
                .build();
        distributedPipelineMetadataManager.save(pipeline);
        pipeline.setName(TestUtils.TEST_PIPELINE_NAME + "-2");
        distributedPipelineMetadataManager.save(pipeline);
        assertEquals(2, distributedPipelineMetadataManager.get().size());
    }


}
