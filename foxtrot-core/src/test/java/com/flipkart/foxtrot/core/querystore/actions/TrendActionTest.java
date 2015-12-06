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
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.google.common.collect.Lists;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class TrendActionTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
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
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        List<Document> documents = TestUtils.getTrendDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
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
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(trendRequest);
    }

    //TODO trend action with null field is not working
    @Test
    public void testTrendActionNullField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField(null);

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
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("all");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        expectedResponse.setTrends(new HashMap<String, List<TrendResponse.Count>>());

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionFieldWithDot() throws QueryStoreException, JsonProcessingException {
        Document document = TestUtils.getDocument("G", 1398653118006L, new Object[]{"data.version", 1}, mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, document);
        elasticsearchServer.getClient().admin().indices()
                .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                .setForce(true)
                .execute()
                .actionGet();
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("data.version");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        TrendResponse.Count count = new TrendResponse.Count();
        count.setPeriod(1398643200000L);
        count.setCount(1);
        expectedResponse.setTrends(Collections.<String, List<TrendResponse.Count>>singletonMap("1", Arrays.asList(count)));

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionEmptyField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
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
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("!@!41242$");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        expectedResponse.setTrends(new HashMap<String, List<TrendResponse.Count>>());

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }


    @Test
    public void testTrendActionNullTable() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        try {
            queryExecutor.execute(trendRequest);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTrendActionWithField() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroTo() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroFrom() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithValues() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Lists.<Filter>newArrayList(betweenFilter));
        trendRequest.setValues(Arrays.asList("android"));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithValues() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");
        trendRequest.setValues(Arrays.asList("android"));

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));


        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithInterval() throws QueryStoreException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");
        trendRequest.setPeriod(Period.days);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = new HashMap<String, List<TrendResponse.Count>>();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }
}
