package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.*;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import io.dropwizard.util.Duration;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/***
 Created by mudit.g on Mar, 2019
 ***/
public class MultiTimeQueryActionTest extends ActionTest {
    @Before
    public void setUp() throws Exception {
        List<Document> documents = TestUtils.getQueryDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .admin()
                .indices()
                .prepareRefresh("*")
                .execute()
                .actionGet();
    }

    @Test(expected = NullPointerException.class)
    public void testQueryException() throws FoxtrotException, JsonProcessingException {
        when(getElasticsearchConnection().getClient()).thenReturn(null);

        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        BetweenFilter betweenFilter = new BetweenFilter("_timestamp", 1397658117000L, 1397658118005L, false);
        query.setFilters(Collections.singletonList(betweenFilter));

        Duration duration = Duration.days(1);
        MultiTimeQueryRequest multiTimeQueryRequest = new MultiTimeQueryRequest(1, duration, query);
        ActionResponse actionResponse = getQueryExecutor().execute(multiTimeQueryRequest);
        MultiTimeQueryResponse multiTimeQueryResponse = null;
        if(actionResponse instanceof MultiTimeQueryResponse) {
            multiTimeQueryResponse = (MultiTimeQueryResponse)actionResponse;
        }
        assertNotNull(multiTimeQueryResponse);

        QueryResponse queryResponse = (QueryResponse)multiTimeQueryResponse.getResponses()
                .get(1);

        assertEquals(11, queryResponse.getTotalHits());
    }

    @Test
    public void testMultiTimeQuery() throws FoxtrotException, JsonProcessingException {
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        BetweenFilter betweenFilter = new BetweenFilter("_timestamp", 1397658117000L, 1397658118005L, false);
        query.setFilters(Arrays.asList(betweenFilter));

        Duration duration = Duration.days(1);
        MultiTimeQueryRequest multiTimeQueryRequest = new MultiTimeQueryRequest(1, duration, query);
        ActionResponse actionResponse = getQueryExecutor().execute(multiTimeQueryRequest);
        MultiTimeQueryResponse multiTimeQueryResponse = null;
        if(actionResponse instanceof MultiTimeQueryResponse) {
            multiTimeQueryResponse = (MultiTimeQueryResponse)actionResponse;
        }
        assertNotNull(multiTimeQueryResponse);

        QueryResponse queryResponse = (QueryResponse)multiTimeQueryResponse.getResponses()
                .get("1397658117000");

        assertEquals(9, queryResponse.getTotalHits());
    }

    @Test
    public void testQueryNoFilterAscending() throws FoxtrotException, JsonProcessingException {

        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        BetweenFilter betweenFilter = new BetweenFilter("_timestamp", 1397658117000L, 1397658118005L, false);
        query.setFilters(Arrays.asList(betweenFilter));

        Duration duration = Duration.days(1);
        MultiTimeQueryRequest multiTimeQueryRequest = new MultiTimeQueryRequest(1, duration, query);

        List<Document> documents = new ArrayList<>();
        documents.add(
                TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, getMapper()));
        documents.add(
                TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, getMapper()));
        documents.add(
                TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, getMapper()));
        documents.add(
                TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, getMapper()));
        documents.add(
                TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, getMapper()));
        documents.add(
                TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, getMapper()));
        documents.add(
                TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, getMapper()));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, getMapper()));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, getMapper()));

        MultiTimeQueryResponse multiTimeQueryResponse = MultiTimeQueryResponse.class.cast(
                getQueryExecutor().execute(multiTimeQueryRequest));
        for(String key : multiTimeQueryResponse.getResponses()
                .keySet()) {
            compare(documents, ((QueryResponse)multiTimeQueryResponse.getResponses()
                    .get(key)).getDocuments());
        }
    }

    public void compare(List<Document> expectedDocuments, List<Document> actualDocuments) {
        assertEquals(expectedDocuments.size(), actualDocuments.size());
        for(int i = 0; i < expectedDocuments.size(); i++) {
            Document expected = expectedDocuments.get(i);
            Document actual = actualDocuments.get(i);
            assertNotNull(expected);
            assertNotNull(actual);
            assertNotNull("Actual document Id should not be null", actual.getId());
            assertNotNull("Actual document data should not be null", actual.getData());
            assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
            assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
            Map<String, Object> expectedMap = getMapper().convertValue(expected.getData(), new TypeReference<HashMap<String, Object>>() {
            });
            Map<String, Object> actualMap = getMapper().convertValue(actual.getData(), new TypeReference<HashMap<String, Object>>() {
            });
            assertEquals("Actual data should match expected data", expectedMap, actualMap);
        }
    }
}
