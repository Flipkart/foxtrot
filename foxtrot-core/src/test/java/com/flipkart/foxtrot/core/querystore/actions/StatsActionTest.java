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
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class StatsActionTest extends ActionTest {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getStatsDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").execute().actionGet();
    }

    @Test
    public void testStatsActionWithoutNesting() throws FoxtrotException, JsonProcessingException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(150, statsResponse.getResult().getStats().get("sum").intValue());
        assertEquals(5, statsResponse.getResult().getStats().get("count").intValue());
        assertNull(statsResponse.getBuckets());
    }

    @Test
    public void testStatsActionWithNesting() throws FoxtrotException, JsonProcessingException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os"));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(3, statsResponse.getBuckets().size());
        for (BucketResponse bucketResponse : statsResponse.getBuckets()) {
            assertNotNull(bucketResponse.getResult());
        }
    }

    @Test
    public void testStatsActionWithMultiLevelNesting() throws FoxtrotException, JsonProcessingException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(3, statsResponse.getBuckets().size());
        for (BucketResponse bucketResponse : statsResponse.getBuckets()) {
            assertNull(bucketResponse.getResult());
            assertNotNull(bucketResponse.getBuckets());
        }
    }


    @Test(expected = MalformedQueryException.class)
    public void testStatsActionNullTable() throws FoxtrotException, JsonProcessingException {
        StatsRequest request = new StatsRequest();
        request.setTable(null);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request);
    }

    @Test(expected = MalformedQueryException.class)
    public void testStatsActionNullField() throws FoxtrotException, JsonProcessingException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField(null);
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request);
    }
}
