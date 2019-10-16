package com.flipkart.foxtrot.core.querystore.actions;/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.*;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/***
 Created by nitish.goyal on 22/08/18
 ***/
public class MultiQueryActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
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

        HashMap<String, ActionRequest> requests = Maps.newHashMap();
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        requests.put("1", query);

        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(false);
        requests.put("2", countRequest);

        MultiQueryRequest multiQueryRequest = new MultiQueryRequest(requests);
        ActionResponse actionResponse = getQueryExecutor().execute(multiQueryRequest);
        MultiQueryResponse multiQueryResponse = null;
        if (actionResponse instanceof MultiQueryResponse) {
            multiQueryResponse = (MultiQueryResponse) actionResponse;
        }
        assertNotNull(multiQueryResponse);

        QueryResponse queryResponse = (QueryResponse) multiQueryResponse.getResponses()
                .get(1);
        CountResponse countResponse = (CountResponse) multiQueryResponse.getResponses()
                .get(2);

        assertEquals(11, countResponse.getCount());
    }

    @Test
    public void testMultiQuery() throws FoxtrotException, JsonProcessingException {

        HashMap<String, ActionRequest> requests = Maps.newHashMap();
        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        requests.put("1", query);

        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(false);
        requests.put("2", countRequest);

        MultiQueryRequest multiQueryRequest = new MultiQueryRequest(requests);
        ActionResponse actionResponse = getQueryExecutor().execute(multiQueryRequest);
        MultiQueryResponse multiQueryResponse = null;
        if (actionResponse instanceof MultiQueryResponse) {
            multiQueryResponse = (MultiQueryResponse) actionResponse;
        }
        assertNotNull(multiQueryResponse);

        QueryResponse queryResponse = (QueryResponse) multiQueryResponse.getResponses()
                .get("1");
        CountResponse countResponse = (CountResponse) multiQueryResponse.getResponses()
                .get("2");

        assertEquals(9, countResponse.getCount());
    }

    @Test
    public void testQueryNoFilterAscending() throws FoxtrotException, JsonProcessingException {
        HashMap<String, ActionRequest> requests = Maps.newHashMap();

        Query query = new Query();
        query.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        requests.put("1", query);
        requests.put("2", query);

        MultiQueryRequest multiQueryRequest = new MultiQueryRequest(requests);

        List<Document> documents = new ArrayList<>();
        documents.add(
                TestUtils.getDocument("W",
                                      1397658117001L,
                                      new Object[]{"os", "android", "device", "nexus", "battery", 99},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("X",
                                      1397658117002L,
                                      new Object[]{"os", "android", "device", "nexus", "battery", 74},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("Y",
                                      1397658117003L,
                                      new Object[]{"os", "android", "device", "nexus", "battery", 48},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("Z",
                                      1397658117004L,
                                      new Object[]{"os", "android", "device", "nexus", "battery", 24},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("A",
                                      1397658118000L,
                                      new Object[]{"os", "android", "version", 1, "device", "nexus"},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("B",
                                      1397658118001L,
                                      new Object[]{"os", "android", "version", 1, "device", "galaxy"},
                                      getMapper()));
        documents.add(
                TestUtils.getDocument("C",
                                      1397658118002L,
                                      new Object[]{"os", "android", "version", 2, "device", "nexus"},
                                      getMapper()));
        documents.add(TestUtils.getDocument("D",
                                            1397658118003L,
                                            new Object[]{"os", "ios", "version", 1, "device", "iphone"},
                                            getMapper()));
        documents.add(TestUtils.getDocument("E",
                                            1397658118004L,
                                            new Object[]{"os", "ios", "version", 2, "device", "ipad"},
                                            getMapper()));

        MultiQueryResponse multiQueryResponse = MultiQueryResponse.class.cast(getQueryExecutor().execute(
                multiQueryRequest));
        for (Map.Entry<String, ActionResponse> response : multiQueryResponse.getResponses()
                .entrySet()) {
            compare(documents, ((QueryResponse) response.getValue()).getDocuments());
        }
    }

    public void compare(List<Document> expectedDocuments, List<Document> actualDocuments) {
        assertEquals(expectedDocuments.size(), actualDocuments.size());
        for (int i = 0; i < expectedDocuments.size(); i++) {
            Document expected = expectedDocuments.get(i);
            Document actual = actualDocuments.get(i);
            assertNotNull(expected);
            assertNotNull(actual);
            assertNotNull("Actual document Id should not be null", actual.getId());
            assertNotNull("Actual document data should not be null", actual.getData());
            assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
            assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(),
                         actual.getTimestamp());
            Map<String, Object> expectedMap = getMapper().convertValue(expected.getData(),
                                                                       new TypeReference<HashMap<String, Object>>() {
                                                                       });
            Map<String, Object> actualMap = getMapper().convertValue(actual.getData(),
                                                                     new TypeReference<HashMap<String, Object>>() {
                                                                     });
            assertEquals("Actual data should match expected data", expectedMap, actualMap);
        }
    }

}
