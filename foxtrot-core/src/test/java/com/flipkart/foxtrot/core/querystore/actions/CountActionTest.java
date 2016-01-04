package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CountActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getCountDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }

    @Test
    public void testCount() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(getQueryExecutor().execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(11, countResponse.getCount());
    }

    @Test
    public void testCountWithFilter() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(getQueryExecutor().execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    @Test
    public void testCountDistinct() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(getQueryExecutor().execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilter() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("device", "nexus"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(getQueryExecutor().execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilterOnSameField() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(getQueryExecutor().execute(countRequest));
        assertNotNull(countResponse);
        assertEquals(1, countResponse.getCount());
    }


}