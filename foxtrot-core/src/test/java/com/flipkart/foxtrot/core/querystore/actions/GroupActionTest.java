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
package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest {
    private QueryExecutor queryExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;

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

        // Ensure that table exists before saving/reading data from it
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        List<Document> documents = TestUtils.getGroupDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testGroupActionSingleQueryException() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));
        when(elasticsearchServer.getClient()).thenReturn(null);
        try {
            queryExecutor.execute(groupRequest);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR, ex.getErrorCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 7L);
        response.put("ios", 4L);

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharacterNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("header.data"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("ios", 1L);

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldEmptyFieldNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList(""));

        try {
            queryExecutor.execute(groupRequest);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharactersNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList(""));

        try {
            queryExecutor.execute(groupRequest);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldHavingSpecialCharactersWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("!@#$%^&*()"));

        Map<String, Object> response = Maps.newHashMap();

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 5L);
        response.put("ios", 1L);

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{put("nexus", 5L); put("galaxy", 2L); }});
        response.put("ios", new HashMap<String, Object>() {{put("nexus", 1L); put("ipad", 2L); put("iphone", 1L); }});

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{put("nexus", 3L); put("galaxy", 2L); }});
        response.put("ios", new HashMap<String, Object>() {{ put("ipad", 1L); }});

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>(){{ put("1", 2L); put("2", 2L); put("3", 1L); }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>(){{ put("2", 1L); put("3", 1L); }};
        response.put("android", new HashMap<String, Object>() {{put("nexus", nexusResponse); put("galaxy", galaxyResponse); }});

        final Map<String, Object> nexusResponse2 = new HashMap<String, Object>(){{ put("2", 1L);}};
        final Map<String, Object> iPadResponse = new HashMap<String, Object>(){{ put("2", 2L); }};
        final Map<String, Object> iPhoneResponse = new HashMap<String, Object>(){{ put("1", 1L); }};
        response.put("ios", new HashMap<String, Object>() {{put("nexus", nexusResponse2); put("ipad", iPadResponse); put("iphone", iPhoneResponse); }});

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>(){{ put("2", 2L); put("3", 1L); }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>(){{ put("2", 1L); put("3", 1L); }};
        response.put("android", new HashMap<String, Object>() {{put("nexus", nexusResponse); put("galaxy", galaxyResponse); }});

        final Map<String, Object> iPadResponse = new HashMap<String, Object>(){{ put("2", 1L); }};
        response.put("ios", new HashMap<String, Object>() {{ put("ipad", iPadResponse); }});

        GroupResponse actualResult = GroupResponse.class.cast(queryExecutor.execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }
}
