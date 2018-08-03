package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public class ActionTest {

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
        this.hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();

        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest().indices(TableMapStore.TABLE_META_INDEX);
        IndicesExistsResponse indicesExistsResponse = elasticsearchConnection.getClient().admin().indices().exists(indicesExistsRequest).actionGet();

        if (!indicesExistsResponse.isExists()) {
            Settings indexSettings = Settings.builder().put("number_of_replicas", 0).build();
            CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
            elasticsearchConnection.getClient().admin().indices().create(createRequest).actionGet();
        }

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        doReturn(true).when(tableMetadataManager).exists(TestUtils.TEST_TABLE_NAME);
        doReturn(TestUtils.TEST_TABLE).when(tableMetadataManager).get(anyString());

        DataStore dataStore = TestUtils.getDataStore();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager,
                dataStore,
                queryStore,
                elasticsearchConnection,
                cacheManager,
                mapper);
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService);
    }

    @After
    public void tearDown() throws Exception {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
            elasticsearchConnection.getClient().admin().indices().delete(deleteIndexRequest);
        } catch (Exception e) {
            //Do Nothing
        }
        elasticsearchConnection.stop();
        getHazelcastInstance().shutdown();
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public ElasticsearchConnection getElasticsearchConnection() {
        return elasticsearchConnection;
    }
}
