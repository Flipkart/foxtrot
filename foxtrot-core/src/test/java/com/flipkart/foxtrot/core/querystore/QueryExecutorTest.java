package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
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
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class QueryExecutorTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutorTest.class.getSimpleName());
    QueryExecutor queryExecutor;
    QueryStore queryStore;
    ObjectMapper mapper = new ObjectMapper();
    MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer();
    String TEST_APP = "test-app";

    @Before
    public void setUp() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = getDataStore();

        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        registerActions(analyticsLoader);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService);

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TEST_APP)).thenReturn(true);

        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void testQueryNoFilterAscending() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - No Filter - Sort Ascending");
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        Query query = new Query();
        query.setTable(TEST_APP);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}," +
                "{\"id\":\"B\",\"timestamp\":1397658118001,\"data\":{\"os\":\"android\",\"device\":\"galaxy\",\"version\":1}}," +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}," +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}" +
                "]}";
        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Sort Ascending");
    }

    @Test
    public void testQueryNoFilterDescending() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - No Filter - Sort Descending");
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        Query query = new Query();
        query.setTable(TEST_APP);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}," +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"B\",\"timestamp\":1397658118001,\"data\":{\"os\":\"android\",\"device\":\"galaxy\",\"version\":1}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}," +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Sort Descending");
    }

    @Test
    public void testQueryNoFilterWithLimit() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        logger.info("Testing Query - No Filter - Limit 2");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(2);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Limit 2");
    }

    @Test
    public void testQueryAnyFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Any Filter");
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        AnyFilter filter = new AnyFilter();
        query.setFilters(Collections.<Filter>singletonList(filter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}," +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"B\",\"timestamp\":1397658118001,\"data\":{\"os\":\"android\",\"device\":\"galaxy\",\"version\":1}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}," +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Any Filter");
    }

    @Test
    public void testQueryEqualsFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - equals Filter");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - equals Filter");
    }

    @Test
    public void testQueryNotEqualsFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - not_equals Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
        notEqualsFilter.setField("os");
        notEqualsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(notEqualsFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"B\",\"timestamp\":1397658118001,\"data\":{\"os\":\"android\",\"device\":\"galaxy\",\"version\":1}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - not_equals Filter");
    }

    @Test
    public void testQueryGreaterThanFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - greater_than Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_than Filter");
    }

    @Test
    public void testQueryGreaterEqualFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - greater_equal Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterEqualFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_equal Filter");
    }

    @Test
    public void testQueryLessThanFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - less_than Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setField("battery");
        lessThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessThanFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - less_than Filter");
    }

    @Test
    public void testQueryLessEqualFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - greater_equal Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessEqualFilter lessEqualFilter = new LessEqualFilter();
        lessEqualFilter.setField("battery");
        lessEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessEqualFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_equal Filter");
    }

    @Test
    public void testQueryBetweenFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - between Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
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

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - between Filter");
    }

    @Test
    public void testQueryContainsFilter() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - contains Filter");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("os");
        containsFilter.setExpression(".*droid.*");
        query.setFilters(Collections.<Filter>singletonList(containsFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"B\",\"timestamp\":1397658118001,\"data\":{\"os\":\"android\",\"device\":\"galaxy\",\"version\":1}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}," +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - contains Filter");
    }

    @Test
    public void testQueryEmptyResult() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - Empty Result Test");
        Query query = new Query();
        query.setTable(TEST_APP);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("wp8");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Empty Result Test");
    }

    @Test
    public void testQueryMultipleFiltersEmptyResult() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - Multiple Filters - Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

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

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Empty Result");
    }

    @Test
    public void testQueryMultipleFiltersAndCombiner() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        logger.info("Testing Query - Multiple Filters - Non Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

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

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Non Empty Result");
    }

    @Test
    public void testQueryMultipleFiltersOrCombiner() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        logger.info("Testing Query - Multiple Filters - Or Combiner - Non Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

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

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"E\",\"timestamp\":1397658118004,\"data\":{\"os\":\"ios\",\"device\":\"ipad\",\"version\":2}}," +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}," +
                "{\"id\":\"C\",\"timestamp\":1397658118002,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":2}}," +
                "{\"id\":\"A\",\"timestamp\":1397658118000,\"data\":{\"os\":\"android\",\"device\":\"nexus\",\"version\":1}}," +
                "{\"id\":\"W\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":99,\"device\":\"nexus\"}}," +
                "{\"id\":\"X\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":74,\"device\":\"nexus\"}}," +
                "{\"id\":\"Y\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":48,\"device\":\"nexus\"}}," +
                "{\"id\":\"Z\",\"timestamp\":1397658117000,\"data\":{\"os\":\"android\",\"battery\":24,\"device\":\"nexus\"}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Or Combiner - Non Empty Result");
    }

    @Test
    public void testQueryPagination() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);

        logger.info("Testing Query - Filter with Pagination");
        Query query = new Query();
        query.setTable(TEST_APP);

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

        ActionResponse response = queryExecutor.execute(query);
        String expectedResponse = "{\"opcode\":\"QueryResponse\",\"documents\":[" +
                "{\"id\":\"D\",\"timestamp\":1397658118003,\"data\":{\"os\":\"ios\",\"device\":\"iphone\",\"version\":1}}" +
                "]}";

        String actualResponse = mapper.writeValueAsString(response);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Filter with Pagination");
    }

    //TODO
    @Test
    public void testGroupActionNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - No Filter");
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TEST_APP);
        groupRequest.setFilters(new ArrayList<Filter>());
        groupRequest.setNesting(Arrays.asList("os"));

        String expectedResult = "{\"opcode\":\"GroupResponse\",\"result\":{\"android\":7,\"ios\":2}}";
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - No Filter");
    }

    @Test
    public void testGroupActionSingleField() throws QueryStoreException, JsonProcessingException {
    }

    @Test
    public void testGroupActionMultipleFields() throws QueryStoreException, JsonProcessingException {
    }

    private DataStore getDataStore() throws DataStoreException {
        HTableInterface tableInterface = MockHTable.create();
        HbaseTableConnection tableConnection = Mockito.mock(HbaseTableConnection.class);
        when(tableConnection.getTable()).thenReturn(tableInterface);
        return new HbaseDataStore(tableConnection, new ObjectMapper());
    }

    private List<Document> getTestDocuments(){
        List<Document> documents = new Vector<Document>();

        documents.add(getDocument("Z", 1397658117000L, new Object[]{ "os", "android", "device", "nexus", "battery", 24}));
        documents.add(getDocument("Y", 1397658117000L, new Object[]{ "os", "android", "device", "nexus", "battery", 48}));
        documents.add(getDocument("X", 1397658117000L, new Object[]{ "os", "android", "device", "nexus", "battery", 74}));
        documents.add(getDocument("W", 1397658117000L, new Object[]{ "os", "android", "device", "nexus", "battery", 99}));
        documents.add(getDocument("A", 1397658118000L, new Object[]{ "os", "android", "version", 1, "device", "nexus"}));
        documents.add(getDocument("B", 1397658118001L, new Object[]{ "os", "android", "version", 1, "device", "galaxy"}));
        documents.add(getDocument("C", 1397658118002L, new Object[]{ "os", "android", "version", 2, "device", "nexus"}));
        documents.add(getDocument("D", 1397658118003L, new Object[]{ "os", "ios", "version", 1, "device", "iphone"}));
        documents.add(getDocument("E", 1397658118004L, new Object[]{ "os", "ios", "version", 2, "device", "ipad"}));
        return documents;
    }

    private Document getDocument(String id, long timestamp, Object[] args){
        Map<String, Object> data = new HashMap<String, Object>();
        for ( int i = 0; i < args.length; i+= 2){
            data.put((String) args[i], args[i+1]);
        }
        return new Document(id, timestamp, mapper.valueToTree(data));
    }

    private void registerActions(AnalyticsLoader analyticsLoader) throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actions = reflections.getSubTypesOf(Action.class);
        for(Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if(null == analyticsProvider
                    || null == analyticsProvider.request()
                    || null == analyticsProvider.opcode()
                    || null == analyticsProvider.opcode()
                    || analyticsProvider.opcode().isEmpty()
                    || null == analyticsProvider.response()) {
//                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            } else {
            analyticsLoader.register(new ActionMetadata(
                    analyticsProvider.request(), action,
                    false, analyticsProvider.opcode()));
            }
        }
    }



}
