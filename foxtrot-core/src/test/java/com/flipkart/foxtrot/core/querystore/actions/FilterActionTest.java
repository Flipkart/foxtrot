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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */

public class FilterActionTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
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
        List<Document> documents = TestUtils.getQueryDocuments(mapper);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore)
                .save(TestUtils.TEST_TABLE, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testQueryException() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(query);
    }

    @Test
    public void testQueryNoFilterAscending() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNoFilterDescending() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNoFilterWithLimit() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(2);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryAnyFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        AnyFilter filter = new AnyFilter();
        query.setFilters(Collections.<Filter>singletonList(filter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryEqualsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNotEqualsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
        notEqualsFilter.setField("os");
        notEqualsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(notEqualsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryGreaterThanFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryGreaterEqualFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterEqualFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryLessThanFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setField("battery");
        lessThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessThanFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryLessEqualFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessEqualFilter lessEqualFilter = new LessEqualFilter();
        lessEqualFilter.setField("battery");
        lessEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessEqualFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryBetweenFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setField("battery");
        betweenFilter.setFrom(47);
        betweenFilter.setTo(75);
        query.setFilters(Collections.<Filter>singletonList(betweenFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryContainsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("os");
        containsFilter.setValue("*droid*");
        query.setFilters(Collections.<Filter>singletonList(containsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryEmptyResult() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("wp8");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryMultipleFiltersEmptyResult() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("android");

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(100);

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(greaterEqualFilter);
        query.setFilters(filters);

        ArrayNode arrayNode = factory.arrayNode();
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryMultipleFiltersAndCombiner() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("android");

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(98);

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(greaterEqualFilter);
        query.setFilters(filters);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryMultipleFiltersOrCombiner() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");

        EqualsFilter equalsFilter2 = new EqualsFilter();
        equalsFilter2.setField("device");
        equalsFilter2.setValue("nexus");

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(equalsFilter2);
        query.setFilters(filters);

        query.setCombiner(FilterCombinerType.or);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryPagination() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryAsync() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        AsyncDataToken response = queryExecutor.executeAsync(query);
        Thread.sleep(200);
        ActionResponse actionResponse = CacheUtils.getCacheFor(response.getAction()).get(response.getKey());
        String actualResponse = mapper.writeValueAsString(actionResponse);
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNullFilters() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setFilters(null);
        query.setCombiner(FilterCombinerType.and);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNullCombiner() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setFilters(new ArrayList<Filter>());
        query.setCombiner(null);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testQueryNullSort() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);
        query.setFilters(new ArrayList<Filter>());
        query.setCombiner(FilterCombinerType.and);
        query.setSort(null);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
    }

    //TODO How to verify if cached data is returned.
    @Test
    public void testQueryCaching() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        assertEquals(expectedResponse, mapper.writeValueAsString(queryExecutor.execute(query)));
        assertEquals(expectedResponse, mapper.writeValueAsString(queryExecutor.execute(query)));
    }
}
