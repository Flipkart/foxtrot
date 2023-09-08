package com.flipkart.foxtrot.core.querystore.actions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.OpensearchTuningConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.OpensearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.Getter;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public abstract class ActionTest {

    private static HazelcastInstance hazelcastInstance;
    @Getter
    private static OpensearchConnection opensearchConnection;
    @Getter
    private static ObjectMapper mapper;
    @Getter
    private static QueryStore queryStore;
    @Getter
    private static QueryExecutor queryExecutor;
    @Getter
    private static DistributedTableMetadataManager tableMetadataManager;
    @Getter
    private static CacheManager cacheManager;

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        opensearchConnection = OpensearchTestUtils.getConnection();
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setHost("127.0.0.1");
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(hazelcastInstance.getConfig());
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true", String.valueOf(OpensearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.ensureIndex(opensearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(opensearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        OpensearchUtils.initializeMappings(opensearchConnection.getClient());
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, opensearchConnection, mapper, cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(30)
                .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper,
                TextNodeRemoverConfiguration.builder().build()));
        DataStore dataStore = TestUtils.getDataStore();
        queryStore = new OpensearchQueryStore(tableMetadataManager, opensearchConnection, dataStore, mutators, mapper, cardinalityConfig);
        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore,
                opensearchConnection, cacheManager, mapper, new OpensearchTuningConfig());
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService, Collections.singletonList(new ResponseCacheUpdater(cacheManager)));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        OpensearchTestUtils.cleanupIndices(opensearchConnection);
        opensearchConnection.stop();
    }

}
