package com.flipkart.foxtrot.core.querystore.actions;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.alerts.EmailClient;
import com.flipkart.foxtrot.core.alerts.EmailConfig;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
@Getter
public abstract class ActionTest {

    protected static final int MAX_CARDINALITY = 10000;

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

    @Before
    public void setUp() throws Exception {
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        this.mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        Config config = new Config();
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setHost("127.0.0.1");
        emailConfig.setFrom("noreply@foxtrot.com");
        EmailClient emailClient = Mockito.mock(EmailClient.class);
        when(emailClient.sendEmail(any(String.class), any(String.class), any(String.class))).thenReturn(true);

        this.hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true",
                String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.createTable(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.createTable(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);

        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper,
                cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(30)
                .build());

        DataStore dataStore = TestUtils.getDataStore();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper,
                cardinalityConfig);
        CacheManager cacheManager = new CacheManager(
                new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore,
                elasticsearchConnection,
                cacheManager, mapper, emailConfig, emailClient
        );
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService);
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
