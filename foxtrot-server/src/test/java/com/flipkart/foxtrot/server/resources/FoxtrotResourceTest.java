package com.flipkart.foxtrot.server.resources;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 27/12/15.
 */
public abstract class FoxtrotResourceTest {

    protected final static HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    protected final static JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    protected final static LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment();
    protected static final Environment environment = mock(Environment.class);
    protected final static Bootstrap<FoxtrotServerConfiguration> bootstrap = mock(Bootstrap.class);
    protected static final ObjectMapper objectMapper = new ObjectMapper();
    private static ObjectMapper mapper;

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

        environment.getObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        environment.getObjectMapper()
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        environment.getObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapper()
                .setSubtypeResolver(subtypeResolver);
        environment.jersey()
                .register(new FoxtrotExceptionMapper(mapper));
        mapper = environment.getObjectMapper();

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    private TableMetadataManager tableMetadataManager;
    private HazelcastInstance hazelcastInstance;
    private QueryExecutor queryExecutor;
    private QueryStore queryStore;
    private DataStore dataStore;
    private CacheManager cacheManager;
    private AnalyticsLoader analyticsLoader;
    private ElasticsearchConnection elasticsearchConnection;

    public FoxtrotResourceTest() throws Exception {
        try {
            dataStore = TestUtils.getDataStore();
        }
        catch (FoxtrotException e) {
            e.printStackTrace();
        }

        Config config = new Config();
        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);

        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));

        elasticsearchConnection = ElasticsearchTestUtils.getConnection();

        CardinalityConfig cardinalityConfig = new CardinalityConfig("true",
                                                                    String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper,
                                                                   cardinalityConfig);
        tableMetadataManager.start();
        tableMetadataManager.save(Table.builder()
                                          .name(TestUtils.TEST_TABLE_NAME)
                                          .ttl(7)
                                          .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper,
                                                                                         TextNodeRemoverConfiguration.builder()
                                                                                                 .build()));
        queryStore = new ElasticsearchQueryStore(tableMetadataManager,
                                                 elasticsearchConnection,
                                                 dataStore,
                                                 mutators,
                                                 mapper,
                                                 cardinalityConfig);
        queryStore = spy(queryStore);

        analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, mapper, new ElasticsearchTuningConfig());
        try {
            analyticsLoader.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TestUtils.registerActions(analyticsLoader, mapper);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService, Collections.emptyList());

    }

    @After
    public void tearDown() throws Exception {
        elasticsearchConnection.stop();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
        analyticsLoader.stop();
    }

    public TableMetadataManager getTableMetadataManager() {
        return tableMetadataManager;
    }

    public ElasticsearchConnection getElasticsearchConnection() {
        return elasticsearchConnection;
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
