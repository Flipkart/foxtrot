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
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
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
import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class HistogramActionTest {
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
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        List<Document> documents = TestUtils.getHistogramDocuments(mapper);
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

    @Test(expected = QueryStoreException.class)
    public void testHistogramActionAnyException() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(histogramRequest);
    }

    @Test
    public void testHistogramActionIntervalMinuteNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397651100000L, 2));
        counts.add(new HistogramResponse.Count(1397658060000L, 3));
        counts.add(new HistogramResponse.Count(1397658180000L, 1));
        counts.add(new HistogramResponse.Count(1397758200000L, 1));
        counts.add(new HistogramResponse.Count(1397958060000L, 1));
        counts.add(new HistogramResponse.Count(1398653100000L, 2));
        counts.add(new HistogramResponse.Count(1398658200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalMinuteWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));
        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397651100000L, 1));
        counts.add(new HistogramResponse.Count(1397658060000L, 2));
        counts.add(new HistogramResponse.Count(1397658180000L, 1));
        counts.add(new HistogramResponse.Count(1397958060000L, 1));
        counts.add(new HistogramResponse.Count(1398658200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalHourNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.hours);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397649600000L, 2));
        counts.add(new HistogramResponse.Count(1397656800000L, 4));
        counts.add(new HistogramResponse.Count(1397757600000L, 1));
        counts.add(new HistogramResponse.Count(1397955600000L, 1));
        counts.add(new HistogramResponse.Count(1398650400000L, 2));
        counts.add(new HistogramResponse.Count(1398657600000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalHourWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.hours);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));


        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397649600000L, 1));
        counts.add(new HistogramResponse.Count(1397656800000L, 3));
        counts.add(new HistogramResponse.Count(1397955600000L, 1));
        counts.add(new HistogramResponse.Count(1398657600000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalDayNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.days);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397606400000L, 6));
        counts.add(new HistogramResponse.Count(1397692800000L, 1));
        counts.add(new HistogramResponse.Count(1397952000000L, 1));
        counts.add(new HistogramResponse.Count(1398643200000L, 3));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalDayWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.days);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(queryExecutor.execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397606400000L, 4));
        counts.add(new HistogramResponse.Count(1397952000000L, 1));
        counts.add(new HistogramResponse.Count(1398643200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }
}
