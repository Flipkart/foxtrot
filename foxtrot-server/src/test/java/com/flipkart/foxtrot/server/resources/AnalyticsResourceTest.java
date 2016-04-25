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
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AnalyticsResourceTest extends FoxtrotResourceTest {

    public AnalyticsResourceTest() throws Exception {
        super();
        doReturn(true).when(getTableMetadataManager()).exists(anyString());
        doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(anyString());
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new AnalyticsResource(getQueryExecutor()));
    }


    @Test
    public void testRunSync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>() {{
            put("1", 2);
            put("2", 2);
            put("3", 1);
        }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>() {{
            put("2", 1);
            put("3", 1);
        }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>() {{
            put("2", 1);
        }};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>() {{
            put("2", 2);
        }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>() {{
            put("1", 1);
        }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        WebResource webResource = client().resource("/v1/analytics");
        GroupResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(GroupResponse.class, groupRequest);
        assertEquals(expectedResponse, response.getResult());
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        WebResource webResource = client().resource("/v1/analytics");
        try {
            webResource.type(MediaType.APPLICATION_JSON_TYPE).post(GroupResponse.class, groupRequest);
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testRunSyncAsync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>() {{
            put("1", 2);
            put("2", 2);
            put("3", 1);
        }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>() {{
            put("2", 1);
            put("3", 1);
        }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>() {{
            put("2", 1);
        }};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>() {{
            put("2", 2);
        }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>() {{
            put("1", 1);
        }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        WebResource webResource = client().resource("/v1/analytics/async");
        AsyncDataToken response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(AsyncDataToken.class, groupRequest);
        Thread.sleep(2000);
        GroupResponse actualResponse = GroupResponse.class.cast(getCacheManager().getCacheFor(response.getAction()).get(response.getKey()));

        assertEquals(expectedResponse, actualResponse.getResult());
    }

    @Test
    public void testRunSyncAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GroupResponse expectedResponse = new GroupResponse();
        WebResource webResource = client().resource("/v1/analytics/async");
        AsyncDataToken asyncDataToken = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(AsyncDataToken.class, groupRequest);
        Thread.sleep(2000);

        GroupResponse actualResponse = GroupResponse.class.cast(getCacheManager().getCacheFor(asyncDataToken.getAction()).get(asyncDataToken.getKey()));
        assertEquals(expectedResponse.getResult(), actualResponse.getResult());
    }
}
