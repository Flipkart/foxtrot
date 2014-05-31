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
import com.flipkart.foxtrot.common.histogram.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class TrendActionTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private JsonNodeFactory factory = JsonNodeFactory.instance;
    private ElasticsearchQueryStore queryStore;

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
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor);
        queryStore.save(TestUtils.TEST_TABLE, TestUtils.getTrendDocuments(mapper));
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testTrendActionAnyException() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(trendRequest);
    }

    //TODO trend action with null field is not working
    @Test
    public void testTrendActionNullField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField(null);

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        try {
            queryExecutor.execute(trendRequest);
        } catch (Exception e) {
            assertEquals("Invalid field name", e.getMessage());
            return;
        }
        fail("Should have thrown exception");
    }

    //TODO trend action with all field is not working
    @Test
    public void testTrendActionFieldAll() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField("all");
        trendRequest.setValues(Collections.<String>emptyList());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionFieldWithDot() throws QueryStoreException, JsonProcessingException {
        queryStore.save(TestUtils.TEST_TABLE, (TestUtils.getDocument("G", 1398653118006L, new Object[]{"data.version", 1}, mapper)));
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField("data.version");
        trendRequest.setValues(Collections.<String>emptyList());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("1", factory.arrayNode().add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionEmptyField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField("");
        trendRequest.setValues(Collections.<String>emptyList());
        try {
            queryExecutor.execute(trendRequest);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTrendActionFieldWithSpecialCharacters() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField("!@!41242$");
        trendRequest.setValues(Collections.<String>emptyList());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }


    @Test
    public void testTrendActionNullTable() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        try{
            queryExecutor.execute(trendRequest);
            fail();
        }catch (QueryStoreException ex){
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTrendActionWithField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroTo() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(0L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroFrom() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(0L);
        trendRequest.setField("os");

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithValues() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setValues(Arrays.asList("android"));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithValues() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setValues(Arrays.asList("android"));

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        List<Filter> filters = new ArrayList<Filter>();
        filters.add(equalsFilter);
        trendRequest.setFilters(filters);

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        trendRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 2)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithInterval() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setPeriod(Period.days);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        trendRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 2)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }
}
