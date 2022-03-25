/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.queryexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityValidator;
import com.flipkart.foxtrot.core.cardinality.CardinalityValidatorImpl;
import com.flipkart.foxtrot.core.common.RequestWithNoAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.events.EventIngestionClient;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.persistence.ElasticsearchFunnelStore;
import com.flipkart.foxtrot.core.funnel.services.MappingService;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(RestHighLevelClient.class)
public class QueryExecutorTest {

    private static HazelcastInstance hazelcastInstance;
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private AnalyticsLoader analyticsLoader;
    private QueryExecutorFactory queryExecutorFactory;
    private ExecutorService executorService;
    private FunnelConfiguration funnelConfiguration;
    private ElasticsearchFunnelStore funnelStore;
    private CacheManager cacheManager;
    private RestHighLevelClient restHighLevelClient;

    @BeforeClass
    public static void setupClass() {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        ElasticsearchConnection elasticsearchConnection = PowerMockito.mock(ElasticsearchConnection.class);
        restHighLevelClient = PowerMockito.mock(RestHighLevelClient.class);
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        PowerMockito.when(elasticsearchConnection.getClient())
                .thenReturn(restHighLevelClient);
        PowerMockito.when(elasticsearchConnection.getConfig())
                .thenReturn(elasticsearchConfig);

        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        HazelcastConnection hazelcastConnection = mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        this.cacheManager = new CacheManager(
                new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        TableMetadataManager tableMetadataManager = mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(ArgumentMatchers.anyString())).thenReturn(true);
        when(tableMetadataManager.get(ArgumentMatchers.anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = mock(QueryStore.class);
        CardinalityValidator cardinalityValidator = new CardinalityValidatorImpl(queryStore, tableMetadataManager);

        analyticsLoader = spy(
                new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager,
                        mapper, new ElasticsearchTuningConfig(), cardinalityValidator));
        TestUtils.registerActions(analyticsLoader, mapper);

        SerDe.init(mapper);
        this.executorService = Executors.newFixedThreadPool(1);

        this.funnelConfiguration = FunnelConfiguration.builder()
                .baseFunnelEventConfig(BaseFunnelEventConfig.builder()
                        .eventType("APP_LOADED")
                        .category("APP_LOADED")
                        .build())
                .build();

        EventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());
        EventBusManager eventBusManager = new EventBusManager(eventBus, new EventIngestionClient());
        eventBusManager.start();

        QueryConfig queryConfig = QueryConfig.builder()
                .slowQueryThresholdMs(0)
                .timeoutExceptionMessages(Collections.singletonList("listener timeout after waiting"))
                .build();

        List<ActionExecutionObserver> actionExecutionObservers = ImmutableList.<ActionExecutionObserver>builder().add(
                new MetricRecorder())
                .add(new ResponseCacheUpdater(cacheManager))
                .add(new SlowQueryReporter(queryConfig))
                .add(new EventPublisherActionExecutionObserver(new GuavaInternalEventBus(), eventBusManager,
                        queryConfig))
                .build();

        queryExecutor = new SimpleQueryExecutor(analyticsLoader, executorService, actionExecutionObservers);
        MappingService mappingService = new MappingService(elasticsearchConnection, funnelConfiguration);
        this.funnelStore = new ElasticsearchFunnelStore(elasticsearchConnection, mappingService, funnelConfiguration);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testResolve() throws Exception {
        assertEquals(NonCacheableAction.class, queryExecutor.resolve(new NonCacheableActionRequest())
                .getClass());
    }

    @Test
    public void testResolveNonExistentAction() throws Exception {
        try {
            queryExecutor.resolve(new RequestWithNoAction());
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.UNRESOLVABLE_OPERATION, e.getCode());
        }
    }

    @Test
    public void testSimpleExecutorFactory() {
        ActionRequest actionRequest = new Query();
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testSimpleExecutorFactoryWithFlagEnabled() {
        ActionRequest actionRequest = new Query();
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testExtrapolationQueryExecutorFactoryWithQuery() {
        ActionRequest actionRequest = new Query();
        Filter filter = EqualsFilter.builder()
                .field(FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD)
                .value("123")
                .build();
        actionRequest.getFilters()
                .add(filter);
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    //TODO Fix this test
    public void testExtrapolationQueryExecutorFactoryWithCountRequest() {
        Filter filter = EqualsFilter.builder()
                .field(FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD)
                .value("123")
                .build();
        List<Filter> filters = new ArrayList<>();
        filters.add(filter);
        ActionRequest actionRequest = CountRequest.builder().filters(filters).build();
        actionRequest.setExtrapolationFlag(true);
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        Assert.assertTrue(queryExecutor instanceof ExtrapolationQueryExecutor);
    }

}
