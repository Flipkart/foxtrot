/**
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
package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.stats.*;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

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
        getElasticsearchConnection().getClient()
                .admin()
                .indices()
                .prepareRefresh("*")
                .execute()
                .actionGet();
    }

    @Test
    public void testStatsActionWithoutNesting() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(150, statsResponse.getResult()
                .getStats()
                .get("sum")
                .intValue());
        assertEquals(5, statsResponse.getResult()
                .getStats()
                .get("count")
                .intValue());
        assertNull(statsResponse.getBuckets());
        assertNotNull(statsResponse.getResult().getPercentiles());
    }

    @Test
    public void testStatsActionWithoutNestingNoPercentile() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setFlags(EnumSet.of(AnalyticsRequestFlags.STATS_SKIP_PERCENTILES));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(150, statsResponse.getResult()
                .getStats()
                .get("sum")
                .intValue());
        assertEquals(5, statsResponse.getResult()
                .getStats()
                .get("count")
                .intValue());
        assertNull(statsResponse.getBuckets());
        assertNull(statsResponse.getResult().getPercentiles());
    }

    @Test
    public void testStatsActionWithoutNestingNonNumericField() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("os");
        request.setFilters(Collections.singletonList(new EqualsFilter("os", "android")));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(2, statsResponse.getResult()
                .getStats()
                .get("count")
                .intValue());
        assertNull(statsResponse.getBuckets());
        assertNull(statsResponse.getResult().getPercentiles());
    }

    @Test
    public void testStatsActionNoExtendedStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(EnumSet.allOf(Stat.class)
                                 .stream()
                                 .filter(x -> !x.isExtended())
                                 .collect(Collectors.toSet()));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(5, statsResponse.getResult()
                .getStats()
                .size());
    }

    @Test
    public void testStatsActionOnlyCountStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.COUNT));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(1, statsResponse.getResult()
                .getStats()
                .size());
        assertTrue(statsResponse.getResult()
                           .getStats()
                           .containsKey("count"));
    }

    @Test
    public void testStatsActionOnlyMaxStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.MAX));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(1, statsResponse.getResult()
                .getStats()
                .size());
        assertTrue(statsResponse.getResult()
                           .getStats()
                           .containsKey("max"));
    }

    @Test
    public void testStatsActionOnlyMinStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.MIN));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(1, statsResponse.getResult()
                .getStats()
                .size());
        assertTrue(statsResponse.getResult()
                           .getStats()
                           .containsKey("min"));
    }


    @Test
    public void testStatsActionOnlyAvgStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.AVG));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(1, statsResponse.getResult()
                .getStats()
                .size());
        assertTrue(statsResponse.getResult()
                           .getStats()
                           .containsKey("avg"));
    }

    @Test
    public void testStatsActionOnlySumStat() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.SUM));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertTrue(statsResponse.getResult()
                           .getStats()
                           .containsKey("sum"));
    }

    @Test
    public void testStatsActionOnlyOnePercentile() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setPercentiles(Collections.singletonList(5d));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsResponse statsResponse = (StatsResponse) getQueryExecutor().execute(request);
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(1, statsResponse.getResult()
                .getPercentiles()
                .size());
        assertTrue(statsResponse.getResult()
                           .getPercentiles()
                           .containsKey(5d));
    }

    @Test
    public void testStatsActionWithNesting() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os"));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(3, statsResponse.getBuckets()
                .size());
        for (BucketResponse bucketResponse : statsResponse.getBuckets()) {
            assertNotNull(bucketResponse.getResult());
        }
    }

    @Test
    public void testStatsActionWithMultiLevelNesting() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));

        StatsResponse statsResponse = StatsResponse.class.cast(getQueryExecutor().execute(request));
        assertNotNull(statsResponse);
        assertNotNull(statsResponse.getResult());
        assertEquals(3, statsResponse.getBuckets()
                .size());
        for (BucketResponse bucketResponse : statsResponse.getBuckets()) {
            assertNull(bucketResponse.getResult());
            assertNotNull(bucketResponse.getBuckets());
        }
    }


    @Test(expected = MalformedQueryException.class)
    public void testStatsActionNullTable() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(null);
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request);
    }

    @Test(expected = MalformedQueryException.class)
    public void testStatsActionNullField() throws FoxtrotException {
        StatsRequest request = new StatsRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setField(null);
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request);
    }
}
