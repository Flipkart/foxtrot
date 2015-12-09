/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.*;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
public class QueryExecutorTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private AnalyticsLoader analyticsLoader;

    @Before
    public void setUp() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());
        TableMetadataManager tableMetadataManager = mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(eq(TestUtils.TEST_TABLE_NAME))).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager.start();
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);

        analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        List<Document> documents = TestUtils.getGroupDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }

        analyticsLoader = spy(new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection));
        TestUtils.registerActions(analyticsLoader, mapper);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);

    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testResolve() throws Exception {
        assertEquals(NonCacheableAction.class, queryExecutor.resolve(new NonCacheableActionRequest()).getClass());
    }

    @Test(expected = QueryStoreException.class)
    public void testResolveNonExistentAction() throws Exception {
        queryExecutor.resolve(new RequestWithNoAction());
    }

    @Test(expected = QueryStoreException.class)
    public void testResolveLoaderException() throws Exception {
        doThrow(new IOException()).when(analyticsLoader).getAction(any(ActionRequest.class));
        queryExecutor.resolve(new NonCacheableActionRequest());
    }

    @Test
    public void testExecute() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = getExpectedResponse();

        GroupResponse response = (GroupResponse) queryExecutor.execute(groupRequest);
        assertEquals(mapper.readValue(mapper.writeValueAsBytes(expectedResponse), Map.class), mapper.readValue(mapper.writeValueAsBytes(response.getResult()), Map.class));
    }

    @Test(expected = QueryStoreException.class)
    public void testExecuteInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy-32");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        try {
            queryExecutor.execute(groupRequest);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testExecuteNullTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(null);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        try {
            queryExecutor.execute(groupRequest);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testExecuteAsync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = getExpectedResponse();

        AsyncDataToken response = queryExecutor.executeAsync(groupRequest);
        Thread.sleep(2000);
        GroupResponse actualResponse = GroupResponse.class.cast(CacheUtils.getCacheFor(response.getAction()).get(response.getKey()));

        assertEquals(mapper.readValue(mapper.writeValueAsBytes(expectedResponse), Map.class), mapper.readValue(mapper.writeValueAsBytes(actualResponse.getResult()), Map.class));
    }



    @Test(expected = QueryStoreException.class)
    public void testExecuteAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy-32");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        try {
            queryExecutor.executeAsync(groupRequest);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testExecuteAsyncNullTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(null);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        try {
            queryExecutor.executeAsync(groupRequest);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    private Map<String, Object> getExpectedResponse() {
        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        return expectedResponse;
    }
}
