package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.yammer.dropwizard.testing.ResourceTest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 27/12/15.
 */
public abstract class FoxtrotResourceTest extends ResourceTest {

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private QueryExecutor queryExecutor;
    private QueryStore queryStore;
    private DataStore dataStore;
    private ObjectMapper mapper;
    private CacheManager cacheManager;

    public FoxtrotResourceTest() throws Exception {
        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        getObjectMapperFactory().setSubtypeResolver(subtypeResolver);

        getObjectMapperFactory().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        this.mapper = getObjectMapperFactory().build();
        this.dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        this.cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        tableMetadataManager.start();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper);
        this.queryStore = spy(queryStore);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, mapper);
        analyticsLoader.start();
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }

    public TableMetadataManager getTableMetadataManager() {
        return tableMetadataManager;
    }

    public MockElasticsearchServer getElasticsearchServer() {
        return elasticsearchServer;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    public DataStore getDataStore() {
        return dataStore;
    }
}
