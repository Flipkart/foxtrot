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
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class TrendActionTest extends ActionTest {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getTrendDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }

    @Test(expected = FoxtrotException.class)
    public void testTrendActionAnyException() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        doReturn(null).when(getElasticsearchConnection()).getClient();
        getQueryExecutor().execute(trendRequest);
    }

    //TODO trend action with null field is not working
    @Test
    public void testTrendActionNullField() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField(null);

        try {
            getQueryExecutor().execute(trendRequest);
            fail("Should have thrown exception");
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.MALFORMED_QUERY, e.getCode());
        }
    }

    //TODO trend action with all field is not working
    @Test
    public void testTrendActionFieldAll() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("all");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        expectedResponse.setTrends(new HashMap<>());

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test

    public void testTrendActionFieldWithDot() throws FoxtrotException {
        Document document = TestUtils.getDocument("G", 1398653118006L, new Object[]{"data.version", 1}, getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, document);
        getElasticsearchServer().getClient().admin().indices()
                .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                .setForce(true)
                .execute()
                .actionGet();
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("data.version");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        TrendResponse.Count count = new TrendResponse.Count();
        count.setPeriod(1398643200000L);
        count.setCount(1);
        expectedResponse.setTrends(Collections.<String, List<TrendResponse.Count>>singletonMap("1", Arrays.asList(count)));
        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionEmptyField() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("");
        trendRequest.setValues(Collections.<String>emptyList());
        try {
            getQueryExecutor().execute(trendRequest);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testTrendActionFieldWithSpecialCharacters() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        trendRequest.setField("!@!41242$");
        trendRequest.setValues(Collections.<String>emptyList());

        TrendResponse expectedResponse = new TrendResponse();
        expectedResponse.setTrends(new HashMap<>());

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }


    @Test
    public void testTrendActionNullTable() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));
        try {
            getQueryExecutor().execute(trendRequest);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testTrendActionWithField() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroTo() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldZeroFrom() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Collections.<Filter>singletonList(betweenFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        counts.add(new TrendResponse.Count(1397952000000L, 1));
        counts.add(new TrendResponse.Count(1398643200000L, 2));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithValues() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        trendRequest.setField("os");
        trendRequest.setFilters(Lists.<Filter>newArrayList(betweenFilter));
        trendRequest.setValues(Arrays.asList("android"));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 6));
        counts.add(new TrendResponse.Count(1398643200000L, 1));
        trends.put("android", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithValues() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");
        trendRequest.setValues(Arrays.asList("android"));

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));


        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilter() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithInterval() throws FoxtrotException, JsonProcessingException {
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE_NAME);
        trendRequest.setField("os");
        trendRequest.setPeriod(Period.days);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        trendRequest.setFilters(Lists.newArrayList(equalsFilter, lessThanFilter));

        TrendResponse expectedResponse = new TrendResponse();
        Map<String, List<TrendResponse.Count>> trends = Maps.newHashMap();

        List<TrendResponse.Count> counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397606400000L, 2));
        trends.put("android", counts);

        counts = Lists.newArrayList();
        counts.add(new TrendResponse.Count(1397692800000L, 1));
        trends.put("ios", counts);

        expectedResponse.setTrends(trends);

        TrendResponse actualResponse = TrendResponse.class.cast(getQueryExecutor().execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
    }
}
