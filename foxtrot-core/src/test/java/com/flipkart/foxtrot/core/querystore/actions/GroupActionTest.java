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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest {
    private QueryExecutor queryExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private JsonNodeFactory factory = JsonNodeFactory.instance;

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
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE)).thenReturn(true);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor)
                .save(TestUtils.TEST_TABLE, TestUtils.getGroupDocuments(mapper));
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testGroupActionSingleQueryException() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
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
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", 7);
        resultNode.put("ios", 4);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharacterNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("header.data"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("ios", 1);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionSingleFieldEmptyFieldNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList(""));

        try{
            queryExecutor.execute(groupRequest);
            fail();
        } catch (QueryStoreException ex){
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharactersNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList(""));

        try{
            queryExecutor.execute(groupRequest);
            fail();
        } catch (QueryStoreException ex){
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldHavingSpecialCharactersWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("!@#$%^&*()"));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", factory.objectNode());
        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionSingleFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("os"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", 5);
        resultNode.put("ios", 1);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionTwoFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", factory.objectNode().put("nexus", 5).put("galaxy", 2));
        resultNode.put("ios", factory.objectNode().put("nexus", 1).put("ipad", 2).put("iphone", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionTwoFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", factory.objectNode().put("nexus", 3).put("galaxy", 2));
        resultNode.put("ios", factory.objectNode().put("ipad", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionMultipleFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2).put("1", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("2", 1));
        temp.put("ipad", factory.objectNode().put("2", 2));
        temp.put("iphone", factory.objectNode().put("1", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGroupActionMultipleFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("ipad", factory.objectNode().put("2", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }
}
