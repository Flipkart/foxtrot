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
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.group.MetricsAggregation;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }

    @Ignore
    @Test
    public void testGroupActionSingleQueryException() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));
        doReturn(null).when(getElasticsearchConnection()).getClient();
        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.ACTION_EXECUTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 7L);
        response.put("ios", 4L);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharacterNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("header.data"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("ios", 1L);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldEmptyFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharactersNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldHavingSpecialCharactersWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("!@#$%^&*()"));

        Map<String, Object> response = Maps.newHashMap();

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 5L);
        response.put("ios", 1L);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", 5L);
            put("galaxy", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("nexus", 1L);
            put("ipad", 2L);
            put("iphone", 1L);
        }});

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", 3L);
            put("galaxy", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("ipad", 1L);
        }});

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>() {{
            put("1", 2L);
            put("2", 2L);
            put("3", 1L);
        }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>() {{
            put("2", 1L);
            put("3", 1L);
        }};
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new HashMap<String, Object>() {{
            put("2", 1L);
        }};
        final Map<String, Object> iPadResponse = new HashMap<String, Object>() {{
            put("2", 2L);
        }};
        final Map<String, Object> iPhoneResponse = new HashMap<String, Object>() {{
            put("1", 1L);
        }};
        response.put("ios", new HashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>() {{
            put("2", 2L);
            put("3", 1L);
        }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>() {{
            put("2", 1L);
            put("3", 1L);
        }};
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> iPadResponse = new HashMap<String, Object>() {{
            put("2", 1L);
        }};
        response.put("ios", new HashMap<String, Object>() {{
            put("ipad", iPadResponse);
        }});

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(response, actualResult.getResult());
    }

    @Test
      public void testGroupActionWithMetricsSum() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.sum);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        assertEquals(486.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("android")).get("sum").get("value"));
        assertEquals(159.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("ios")).get("sum").get("value"));
    }
    @Test
     public void testGroupActionWithMetricsMax() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.max);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        assertEquals(99.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("android")).get("max").get("value"));
        assertEquals(56.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("ios")).get("max").get("value"));
    }

    @Test
    public void testGroupActionWithMetricsMin() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.min);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        assertEquals(24.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("android")).get("min").get("value"));
        assertEquals(24.0, ((Map<String,Map<String,Object>>)actualResult.getResult().get("ios")).get("min").get("value"));
    }

    @Test
    public void testGroupActionWithMetricsAvg() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.avg);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        Double actualAnd = (Double) ((Map<String, Map<String, Object>>) actualResult.getResult().get("android")).get("avg").get("value");
        assertEquals("69.43", new DecimalFormat("#.00").format(actualAnd));
        Double actualIos = (Double) ((Map<String, Map<String, Object>>) actualResult.getResult().get("ios")).get("avg").get("value");
        assertEquals("39.75", new DecimalFormat("#.00").format(actualIos));
    }

    @Test
    public void testGroupActionWithMetricsStats() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.stats);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        Map<String,Double> actualAnd = (Map<String, Double>) ((Map<String, Map<String, Object>>) actualResult.getResult().get("android")).get("stats").get("value");
        assertEquals("69.43", new DecimalFormat("#.00").format(actualAnd.get("avg")));
        assertEquals("24.00", new DecimalFormat("#.00").format(actualAnd.get("min")));
        assertEquals("486.00", new DecimalFormat("#.00").format(actualAnd.get("sum")));


        Map<String,Double> actualIos = (Map<String, Double>) ((Map<String, Map<String, Object>>) actualResult.getResult().get("ios")).get("stats").get("value");
        assertEquals("39.75", new DecimalFormat("#.00").format(actualIos.get("avg")));
        assertEquals("24.00", new DecimalFormat("#.00").format(actualIos.get("min")));
        assertEquals("159.00", new DecimalFormat("#.00").format(actualIos.get("sum")));
    }

    @Test
    public void testGroupActionWithMetricsPercentiles() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os"));

        MetricsAggregation metricsAggregation = new MetricsAggregation();
        metricsAggregation.setField("battery");
        metricsAggregation.setType(MetricsAggregation.MetricsAggragationType.percentiles);
        groupRequest.setMetric(metricsAggregation);

        GroupResponse actualResult = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        assertEquals(7l, ((Map<String,Objects>)actualResult.getResult().get("android")).get("count"));
        assertEquals(4l, ((Map<String,Objects>)actualResult.getResult().get("ios")).get("count"));
        Map<Double,Double> actualAnd = (Map<Double, Double>) ((Map<String, Map<String, Object>>) actualResult.getResult().get("android")).get("percentiles").get("value");
        Map<String,Double> actualIos = (Map<String, Double>) ((Map<String, Map<String, Object>>) actualResult.getResult().get("ios")).get("percentiles").get("value");

        assertEquals("25.44", new DecimalFormat("#.00").format(actualAnd.get(1.0)));
        assertEquals("31.20", new DecimalFormat("#.00").format(actualAnd.get(5.0)));
        assertEquals("61.00", new DecimalFormat("#.00").format(actualAnd.get(25.0)));
        assertEquals("76.00", new DecimalFormat("#.00").format(actualAnd.get(50.0)));
        assertEquals("82.50", new DecimalFormat("#.00").format(actualAnd.get(75.0)));

        assertEquals("24.33", new DecimalFormat("#.00").format(actualIos.get(1.0)));
        assertEquals("25.65", new DecimalFormat("#.00").format(actualIos.get(5.0)));
        assertEquals("32.25", new DecimalFormat("#.00").format(actualIos.get(25.0)));
        assertEquals("39.50", new DecimalFormat("#.00").format(actualIos.get(50.0)));
        assertEquals("47.00", new DecimalFormat("#.00").format(actualIos.get(75.0)));
    }
}
