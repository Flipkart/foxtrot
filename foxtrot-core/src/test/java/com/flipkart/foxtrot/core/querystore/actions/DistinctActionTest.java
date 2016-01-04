package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DistinctActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getDistinctDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }


    @Test
    public void testDistinctAsc() throws FoxtrotException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        distinctRequest.setNesting(Arrays.asList(resultSort));

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1"));
        listResponse.add(Arrays.asList("2"));
        listResponse.add(Arrays.asList("3"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(getQueryExecutor().execute(distinctRequest));
        assertNotNull(distinctResponse);
        assertEquals(expectedResponse, distinctResponse);
    }

    @Test
    public void testDistinctDesc() throws FoxtrotException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.desc);
        distinctRequest.setNesting(Arrays.asList(resultSort));

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("3"));
        listResponse.add(Arrays.asList("2"));
        listResponse.add(Arrays.asList("1"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(getQueryExecutor().execute(distinctRequest));
        assertNotNull(distinctResponse);
        assertEquals(expectedResponse, distinctResponse);
    }

    @Test
    public void testDistinctMultipleNestingAscAsc() throws FoxtrotException, JsonProcessingException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);

        List<ResultSort> resultSorts = new ArrayList<ResultSort>();

        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        resultSort = new ResultSort();
        resultSort.setField("os");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        distinctRequest.setNesting(resultSorts);

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version", "os"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1", "android"));
        listResponse.add(Arrays.asList("1", "ios"));
        listResponse.add(Arrays.asList("2", "ios"));
        listResponse.add(Arrays.asList("3", "android"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(getQueryExecutor().execute(distinctRequest));
        assertNotNull(distinctResponse);
    }

    @Test
    public void testDistinctMultipleNestingAscDesc() throws FoxtrotException, JsonProcessingException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);

        List<ResultSort> resultSorts = new ArrayList<ResultSort>();

        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        resultSort = new ResultSort();
        resultSort.setField("os");
        resultSort.setOrder(ResultSort.Order.desc);
        resultSorts.add(resultSort);

        distinctRequest.setNesting(resultSorts);

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version", "os"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1", "ios"));
        listResponse.add(Arrays.asList("1", "android"));
        listResponse.add(Arrays.asList("2", "ios"));
        listResponse.add(Arrays.asList("3", "android"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(getQueryExecutor().execute(distinctRequest));
        assertNotNull(distinctResponse);
    }
}