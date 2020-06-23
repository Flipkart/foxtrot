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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setup();
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .admin()
                .indices()
                .prepareRefresh("*")
                .execute()
                .actionGet();
        getTableMetadataManager().getFieldMappings(TestUtils.TEST_TABLE_NAME, true, true);
        ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig()
                .setMaxCardinality(MAX_CARDINALITY);
        getTableMetadataManager().updateEstimationData(TestUtils.TEST_TABLE_NAME, 1397658117000L);
    }

    /*@Ignore
    @Test
    public void testGroupActionSingleQueryException() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("os"));
        doReturn(null).when(getElasticsearchConnection())
                .getClient();
        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.ACTION_EXECUTION_ERROR, ex.getCode());
        }
    }*/

    @Test
    public void testGroupActionSingleFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 7L);
        response.put("ios", 4L);

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldEmptyFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharactersNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldHavingSpecialCharactersWithFilter()
            throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Collections.singletonList("!@#$%^&*()"));

        Map<String, Object> response = Maps.newHashMap();

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
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
        groupRequest.setNesting(Collections.singletonList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 5L);
        response.put("ios", 1L);

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
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

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
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

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
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

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
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

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionDistinctCountAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setUniqueCountOn("device");

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", 1L);
            put("2", 2L);
            put("3", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 1L);
            put("2", 2L);
        }});
        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionMaxAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setStats(Sets.newHashSet(Stat.MAX));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("max", 48.0));
            put("2", ImmutableMap.of("max", 99.0));
            put("3", ImmutableMap.of("max", 87.0));
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("max", 24.0));
            put("2", ImmutableMap.of("max", 56.0));
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("2")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("3")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("1")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("2")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2")).get("max"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionAvgAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setStats(Sets.newHashSet(Stat.AVG));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("avg", 36.0));
            put("2", ImmutableMap.of("avg", 84.33333333333333));
            put("3", ImmutableMap.of("avg", 80.5));
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("avg", 24.0));
            put("2", ImmutableMap.of("avg", 45.0));
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1")).get("avg"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("2")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2")).get("avg"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("3")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3")).get("avg"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("1")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1")).get("avg"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("2")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2")).get("avg"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionSumAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setStats(Sets.newHashSet(Stat.SUM));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("sum", 72.0));
            put("2", ImmutableMap.of("sum", 253.0));
            put("3", ImmutableMap.of("sum", 161.0));
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("sum", 24.0));
            put("2", ImmutableMap.of("sum", 135.0));
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1")).get("sum"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("2")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2")).get("sum"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("3")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3")).get("sum"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("1")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1")).get("sum"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("2")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2")).get("sum"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionCountAggregation(){
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setStats(Sets.newHashSet( Stat.COUNT));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android",new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("count",2 ));
            put("2", ImmutableMap.of("count", 3));
            put("3", ImmutableMap.of("count",2));
        }});
        response.put("ios",new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("count",1));
            put("2", ImmutableMap.of("count", 3));
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("count"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult().get("android"))
                        .get("1")).get("count"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("2")).get("count"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult().get("android"))
                        .get("2")).get("count"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("3")).get("count"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult().get("android"))
                        .get("3")).get("count"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("1")).get("count"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult().get("ios"))
                        .get("1")).get("count"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("2")).get("count"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult().get("ios"))
                        .get("2")).get("count"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionCountAndSumAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setStats(Sets.newHashSet(Stat.SUM, Stat.COUNT));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("avg", 36.0, "min", 24.0, "max", 48.0, "count", 2, "sum", 72.0));
            put("2", ImmutableMap.of("avg", 84.33333333333333, "min", 76.0, "max", 99.0, "count", 3, "sum", 253.0));
            put("3", ImmutableMap.of("avg", 80.5, "min", 74.0, "max", 87.0, "count", 2, "sum", 161.0));
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", ImmutableMap.of("avg", 24.0, "min", 24.0, "max", 24.0, "count", 1, "sum", 24.0));
            put("2", ImmutableMap.of("avg", 45.0, "min", 35.0, "max", 56.0, "count", 3, "sum", 135.0));
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1")).get("avg"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("1")).get("sum"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1")).get("sum"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("2")).get("min"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2")).get("min"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("android")).get("3")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("1")).get("max"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1")).get("max"));
        assertEquals(((Map<String, Object>) ((Map<String, Object>) response.get("ios")).get("2")).get("avg"),
                ((Map<String, Object>) ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2")).get("avg"));
    }
}
