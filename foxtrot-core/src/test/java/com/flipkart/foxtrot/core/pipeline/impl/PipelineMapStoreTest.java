package com.flipkart.foxtrot.core.pipeline.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.pipeline.processor.TestProcessorDefinition;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.google.common.collect.ImmutableList;
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


public class PipelineMapStoreTest {

    public static final String TEST_PIPELINE = "test-pipeline";
    public static final ProcessorDefinition TEST_PROCESSOR_DEFINITION = new TestProcessorDefinition("$.stringField");
    public static final String PIPELINE_META_INDEX = "pipeline-meta";
    public static final String PIPELINE_META_TYPE = "pipeline-meta";
    private static ElasticsearchConnection elasticsearchConnection;
    private ObjectMapper mapper = new ObjectMapper();
    private PipelineMapStore pipelineMapStore;


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
        TestUtils.ensureIndex(elasticsearchConnection, PipelineMapStore.PIPELINE_META_INDEX);
        mapper.registerSubtypes(new NamedType(TestProcessorDefinition.class, "TEST"));
        PipelineMapStore.Factory factory = new PipelineMapStore.Factory(elasticsearchConnection);
        pipelineMapStore = factory.newMapStore(null, null);
        SerDe.init(mapper);
    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }

    @Test
    public void testStore() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(TEST_PIPELINE);
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        pipelineMapStore.store(pipeline.getName(), pipeline);

        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, pipeline.getName()),
                        RequestOptions.DEFAULT);
        comparePipelines(pipeline, mapper.readValue(response.getSourceAsBytes(), Pipeline.class));
    }

    private void comparePipelines(Pipeline expected,
                                  Pipeline actual) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertTrue(CollectionUtils.isEqualCollection(new ArrayList<>(Arrays.asList(TEST_PROCESSOR_DEFINITION)),
                actual.getProcessors()));
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullKey() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(TEST_PIPELINE);
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelineMapStore.store(null, pipeline);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullPipeline() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(TEST_PIPELINE);
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelineMapStore.store(pipeline.getName(), null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullPipelineName() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(null);
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelineMapStore.store(TEST_PIPELINE, pipeline);
    }

    @Test
    public void testStoreAll() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
        }
        pipelineMapStore.storeAll(pipelines);
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        pipelines.keySet()
                .forEach(key -> multiGetRequest.add(PIPELINE_META_INDEX, PIPELINE_META_TYPE, key));
        MultiGetResponse response = elasticsearchConnection.getClient()
                .mget(multiGetRequest, RequestOptions.DEFAULT);
        Map<String, Pipeline> responsePipelines = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            Pipeline pipeline = mapper.readValue(multiGetItemResponse.getResponse()
                    .getSourceAsString(), Pipeline.class);
            responsePipelines.put(pipeline.getName(), pipeline);
        }
        for (Map.Entry<String, Pipeline> entry : pipelines.entrySet()) {
            comparePipelines(entry.getValue(), responsePipelines.get(entry.getKey()));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNull() throws Exception {
        pipelineMapStore.storeAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullPipelineKey() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(null, pipeline);
        }
        pipelineMapStore.storeAll(pipelines);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullPipelineValue() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            pipelines.put(UUID.randomUUID()
                    .toString(), null);
        }
        pipelineMapStore.storeAll(pipelines);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullPipelineKeyValue() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            pipelines.put(null, null);
        }
        pipelineMapStore.storeAll(pipelines);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeys() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
        }

        Pipeline pipeline = new Pipeline();
        pipeline.setName(UUID.randomUUID()
                .toString());
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelines.put(null, pipeline);
        pipelineMapStore.storeAll(pipelines);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullValues() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
        }

        Pipeline pipeline = new Pipeline();
        pipeline.setName(UUID.randomUUID()
                .toString());
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelines.put(pipeline.getName(), null);
        pipelineMapStore.storeAll(pipelines);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeyValues() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
        }
        pipelines.put(null, null);
        pipelineMapStore.storeAll(pipelines);
    }

    @Test
    public void testDelete() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(TEST_PIPELINE);
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        pipelineMapStore.store(pipeline.getName(), pipeline);
        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, pipeline.getName()),
                        RequestOptions.DEFAULT);
        assertTrue(response.isExists());

        pipelineMapStore.delete(pipeline.getName());
        response = elasticsearchConnection.getClient()
                .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, pipeline.getName()),
                        RequestOptions.DEFAULT);
        assertFalse(response.isExists());
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteNullKey() throws Exception {
        pipelineMapStore.delete(null);
    }

    @Test
    public void testDeleteAll() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
        }
        pipelineMapStore.storeAll(pipelines);
        for (String name : pipelines.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertTrue(response.isExists());
        }

        pipelineMapStore.deleteAll(pipelines.keySet());
        for (String name : pipelines.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertFalse(response.isExists());
        }

    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNull() throws Exception {
        pipelineMapStore.deleteAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNullKeys() throws Exception {
        List<String> keys = new ArrayList<String>();
        keys.add(null);
        keys.add(null);
        pipelineMapStore.deleteAll(keys);
    }

    @Test
    public void testLoad() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(UUID.randomUUID()
                .toString());
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        ;
        Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, pipeline);
        elasticsearchConnection.getClient()
                .index(new IndexRequest(PIPELINE_META_INDEX).type(PIPELINE_META_TYPE)
                        .source(sourceMap)
                        .id(pipeline.getName())
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        Pipeline responsePipeline = pipelineMapStore.load(pipeline.getName());
        comparePipelines(pipeline, responsePipeline);
    }

    @Test
    public void testLoadAll() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString() + "-" + i);
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, pipeline);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(PIPELINE_META_INDEX).type(PIPELINE_META_TYPE)
                            .source(sourceMap)
                            .id(pipeline.getName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> names = ImmutableSet.copyOf(Iterables.limit(pipelines.keySet(), 5));
        Map<String, Pipeline> responsePipelines = pipelineMapStore.loadAll(names);
        assertEquals(names.size(), responsePipelines.size());
        for (String name : names) {
            comparePipelines(pipelines.get(name), responsePipelines.get(name));
        }
    }

    @Test(expected = NullPointerException.class)
    public void testLoadAllNull() throws Exception {
        pipelineMapStore.loadAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadAllKeyWithWrongJson() throws Exception {
        elasticsearchConnection.getClient()
                .index(new IndexRequest(PIPELINE_META_INDEX).type(PIPELINE_META_TYPE)
                        .source("{ \"test\" : \"test\"}")
                        .id(TEST_PIPELINE)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        pipelineMapStore.loadAll(Arrays.asList(TEST_PIPELINE));
    }

    @Test
    public void testLoadAllKeys() throws Exception {
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = new Pipeline();
            pipeline.setName(UUID.randomUUID()
                    .toString());
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            ;
            pipelines.put(pipeline.getName(), pipeline);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, pipeline);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(PIPELINE_META_INDEX).type(PIPELINE_META_TYPE)
                            .source(sourceMap)
                            .id(pipeline.getName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> responseKeys = pipelineMapStore.loadAllKeys();
        for (String name : pipelines.keySet()) {
            assertTrue(responseKeys.contains(name));
        }
    }
}
