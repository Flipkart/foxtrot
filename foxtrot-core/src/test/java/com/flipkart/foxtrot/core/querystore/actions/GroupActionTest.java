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
}
