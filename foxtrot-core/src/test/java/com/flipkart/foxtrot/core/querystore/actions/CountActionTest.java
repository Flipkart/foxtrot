package com.flipkart.foxtrot.core.querystore.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.TestUtils;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Before;
import org.junit.Test;

public class CountActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setup();
        List<Document> documents = TestUtils.getCountDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
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
