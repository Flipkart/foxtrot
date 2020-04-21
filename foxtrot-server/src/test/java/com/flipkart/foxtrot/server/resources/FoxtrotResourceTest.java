package com.flipkart.foxtrot.server.resources;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
import com.flipkart.foxtrot.core.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutor;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutorFactory;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.queryexecutor.ExtrapolationQueryExecutor;
import com.flipkart.foxtrot.core.queryexecutor.SimpleQueryExecutor;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 27/12/15.
 */
@Slf4j
public abstract class FoxtrotResourceTest {

    protected final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    protected final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    protected final LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment();
    protected final Environment environment = mock(Environment.class);
    protected final Bootstrap<FoxtrotServerConfiguration> bootstrap = mock(Bootstrap.class);
    protected final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final ObjectMapper mapper;
    private final HazelcastInstance hazelcastInstance;
    private final ElasticsearchConnection elasticsearchConnection;
    private final TableMetadataManager tableMetadataManager;
    private final CardinalityConfig cardinalityConfig;
    private final List<IndexerEventMutator> mutators;
    private final CacheManager cacheManager;
    private AnalyticsLoader analyticsLoader;
    private QueryExecutorFactory queryExecutorFactory;
    private QueryStore queryStore;
    private DataStore dataStore;


    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    protected FoxtrotResourceTest() {
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
        mapper = environment.getObjectMapper();
        environment.jersey()
                .register(new FoxtrotExceptionMapper(mapper));

        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        try {
            elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());

        Config config = new Config();
        //Initializing Cache Factory
        HazelcastConnection hazelcastConnection = mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);

        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));

        cardinalityConfig = new CardinalityConfig("true", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper,
                cardinalityConfig);
        try {
            tableMetadataManager.start();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(7)
                .build());

        mutators = Lists.newArrayList(
                new LargeTextNodeRemover(mapper, TextNodeRemoverConfiguration.builder().build()));
        dataStore = TestUtils.getDataStore();
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mutators,
                mapper, cardinalityConfig);
        queryStore = spy(queryStore);
        analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection,
                cacheManager, mapper, new ElasticsearchTuningConfig());
        try {
            analyticsLoader.start();
            TestUtils.registerActions(analyticsLoader, mapper);
        } catch (Exception e) {
            log.error("Error in intialization", e);
            Assert.fail();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        FunnelConfiguration funnelConfiguration = FunnelConfiguration.builder()
                .querySize(100)
                .baseFunnelEventConfig(BaseFunnelEventConfig.builder()
                        .eventType("APP_LOADED")
                        .category("General")
                .build())
                .funnelIndex("foxtrot_funnel")
                .build();
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration);

    }

    protected TableMetadataManager getTableMetadataManager() {
        return tableMetadataManager;
    }

    protected ElasticsearchConnection getElasticsearchConnection() {
        return elasticsearchConnection;
    }

    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    protected QueryExecutorFactory getQueryExecutorFactory() {
        return queryExecutorFactory;
    }

    protected ObjectMapper getMapper() {
        return mapper;
    }

    protected CacheManager getCacheManager() {
        return cacheManager;
    }

    protected QueryStore getQueryStore() {
        return queryStore;
    }

    protected DataStore getDataStore() {
        return dataStore;
    }
}
