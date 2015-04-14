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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */

public class FilterActionTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
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
        List<Document> documents = TestUtils.getQueryDocuments(mapper);
        //when(queryStore.get(anyString(), Matchers.anyListOf(String.class))).thenReturn(documents);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testQueryException() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
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
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryNoFilterDescending() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryNoFilterWithLimit() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(2);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryAnyFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        AnyFilter filter = new AnyFilter();
        query.setFilters(Lists.<Filter>newArrayList(filter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryEqualsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Lists.<Filter>newArrayList(equalsFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryNotEqualsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
        notEqualsFilter.setField("os");
        notEqualsFilter.setValue("ios");
        query.setFilters(Lists.<Filter>newArrayList(notEqualsFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryGreaterThanFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        query.setFilters(Lists.<Filter>newArrayList(greaterThanFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryGreaterEqualFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(48);
        query.setFilters(Lists.<Filter>newArrayList(greaterEqualFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryLessThanFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setField("battery");
        lessThanFilter.setValue(48);
        query.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryLessEqualFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessEqualFilter lessEqualFilter = new LessEqualFilter();
        lessEqualFilter.setField("battery");
        lessEqualFilter.setValue(48);
        query.setFilters(Lists.<Filter>newArrayList(lessEqualFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryBetweenFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setField("battery");
        betweenFilter.setFrom(47);
        betweenFilter.setTo(75);
        query.setFilters(Lists.<Filter>newArrayList(betweenFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryContainsFilter() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("os");
        containsFilter.setValue("*droid*");
        query.setFilters(Lists.<Filter>newArrayList(containsFilter));

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryEmptyResult() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("wp8");
        query.setFilters(Lists.<Filter>newArrayList(equalsFilter));

        List<Document> documents = new ArrayList<Document>();
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryMultipleFiltersEmptyResult() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

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

        List<Document> documents = new ArrayList<Document>();
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryMultipleFiltersAndCombiner() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

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

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Ignore
    @Test
    public void testQueryMultipleFiltersOrCombiner() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

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

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        //compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryPagination() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Lists.<Filter>newArrayList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

//    @Test
//    public void testQueryAsync() throws QueryStoreException, JsonProcessingException, InterruptedException {
//        Query query = new Query();
//        query.setTable(TestUtils.TEST_TABLE_NAME);
//
//        ResultSort resultSort = new ResultSort();
//        resultSort.setOrder(ResultSort.Order.desc);
//        resultSort.setField("_timestamp");
//        query.setSort(resultSort);
//
//        EqualsFilter equalsFilter = new EqualsFilter();
//        equalsFilter.setField("os");
//        equalsFilter.setValue("ios");
//        query.setFilters(Lists.<Filter>newArrayList(equalsFilter));
//
//        query.setFrom(1);
//        query.setLimit(1);
//
//        List<Document> documents = new ArrayList<Document>();
//        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
//        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
//        compare(documents, actualResponse.getDocuments());
//
//        AsyncDataToken response = queryExecutor.executeAsync(query);
//        Thread.sleep(200);
//        ActionResponse actionResponse = CacheUtils.getCacheFor(response.getAction()).get(response.getKey());
//        compare(documents, QueryResponse.class.cast(actionResponse).getDocuments());
//    }

    @Test
    public void testQueryNullFilters() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setFilters(null);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Ignore
    @Test
    public void testQueryNullCombiner() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setFilters(new ArrayList<Filter>());
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    @Test
    public void testQueryNullSort() throws QueryStoreException, JsonProcessingException, InterruptedException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        query.setFilters(new ArrayList<Filter>());
        query.setSort(null);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));

        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    //TODO How to verify if cached data is returned.
    @Test
    public void testQueryCaching() throws QueryStoreException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Lists.<Filter>newArrayList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        List<Document> documents = new ArrayList<Document>();
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        QueryResponse actualResponse = QueryResponse.class.cast(queryExecutor.execute(query));
        compare(documents, actualResponse.getDocuments());
    }

    public void compare(List<Document> expectedDocuments, List<Document> actualDocuments){
        assertEquals(expectedDocuments.size(), actualDocuments.size());
        for (int i = 0 ; i < expectedDocuments.size(); i++){
            Document expected = expectedDocuments.get(i);
            Document actual = actualDocuments.get(i);
            assertNotNull(expected);
            assertNotNull(actual);
            assertNotNull("Actual document Id should not be null", actual.getId());
            assertNotNull("Actual document data should not be null", actual.getData());
            assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
            assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
            Map<String, Object> expectedMap = mapper.convertValue(expected.getData(), new TypeReference<HashMap<String, Object>>() {});
            Map<String, Object> actualMap = mapper.convertValue(actual.getData(), new TypeReference<HashMap<String, Object>>() {});
            assertEquals("Actual data should match expected data", expectedMap, actualMap);
        }
    }
}
