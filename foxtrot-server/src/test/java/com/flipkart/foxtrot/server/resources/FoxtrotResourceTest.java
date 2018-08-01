package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.health.HealthCheckRegistry;
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
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeValidationException;
import org.junit.After;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 27/12/15.
 */
public abstract class FoxtrotResourceTest {

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private QueryExecutor queryExecutor;
    private QueryStore queryStore;
    private DataStore dataStore;
    private static ObjectMapper mapper;
    private CacheManager cacheManager;
    private AnalyticsLoader analyticsLoader;


    protected final static HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    protected final static JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    protected final static LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment();
    protected static final Environment environment = mock(Environment.class);
    protected final static Bootstrap<FoxtrotServerConfiguration> bootstrap = mock(Bootstrap.class);

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.getObjectMapper()).thenReturn(objectMapper);
        when(bootstrap.getObjectMapper()).thenReturn(objectMapper);
        when(environment.getApplicationContext()).thenReturn(new MutableServletContextHandler());
        when(environment.getAdminContext()).thenReturn(new MutableServletContextHandler());
        when(environment.getValidator()).thenReturn(Validators.newValidator());

        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        environment.getObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        environment.getObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapper().setSubtypeResolver(subtypeResolver);
        environment.jersey().register(new FoxtrotExceptionMapper(mapper));
        mapper = environment.getObjectMapper();

    }

    public FoxtrotResourceTest() throws NodeValidationException {
        try {
            dataStore = TestUtils.getDataStore();
        } catch (FoxtrotException e) {
            e.printStackTrace();
        }

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = Settings.builder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        try {
            tableMetadataManager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper);
        queryStore = spy(queryStore);

        analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, mapper);
        try {
            analyticsLoader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TestUtils.registerActions(analyticsLoader, mapper);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);

    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
        analyticsLoader.stop();
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
