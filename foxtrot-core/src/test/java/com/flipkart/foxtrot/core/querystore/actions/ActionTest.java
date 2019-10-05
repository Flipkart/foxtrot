package com.flipkart.foxtrot.core.querystore.actions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.Getter;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
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
@Getter
public abstract class ActionTest {

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    private ObjectMapper mapper;
    private QueryStore queryStore;
    private QueryExecutor queryExecutor;
    private HazelcastInstance hazelcastInstance;
    private ElasticsearchConnection elasticsearchConnection;
    private DistributedTableMetadataManager tableMetadataManager;
    private CacheManager cacheManager;

    @Before
    public void setUp() throws Exception {
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        this.mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        Config config = new Config();
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setHost("127.0.0.1");
        this.hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        PutIndexTemplateRequest putIndexTemplateRequest = ElasticsearchUtils.getClusterTemplateMapping();
        AcknowledgedResponse response = elasticsearchConnection.getClient()
                .admin()
                .indices()
                .putTemplate(putIndexTemplateRequest)
                .actionGet();
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper, cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(30)
                .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper,
                TextNodeRemoverConfiguration.builder().build()));
        DataStore dataStore = TestUtils.getDataStore();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mutators, mapper, cardinalityConfig);
        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection,
                                                              cacheManager, mapper);
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService, Collections.singletonList(new ResponseCacheUpdater(cacheManager)));
    }

    @After
    public void tearDown() throws Exception {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
            elasticsearchConnection.getClient()
                    .admin()
                    .indices()
                    .delete(deleteIndexRequest);
        } catch (Exception e) {
            //Do Nothing
        }
        elasticsearchConnection.stop();
        getHazelcastInstance().shutdown();
    }
}
