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
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AsyncResourceTest extends FoxtrotResourceTest {

    public AsyncResourceTest() throws Exception {
        super();
        doReturn(true).when(getTableMetadataManager()).exists(anyString());
        doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(anyString());
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }


    @Override
    protected void setUpResources() throws Exception {
        addResource(new AsyncResource(getCacheManager()));
    }


    @Test
    public void testGetResponse() throws Exception {
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

        AsyncDataToken dataToken = getQueryExecutor().executeAsync(groupRequest);
        Thread.sleep(1000);

        WebResource webResource = client().resource("/v1/async/" + dataToken.getAction() + "/" + dataToken.getKey());
        GroupResponse groupResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);

        assertEquals(expectedResponse, groupResponse.getResult());
    }

    @Test
    public void testGetResponseInvalidAction() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        AsyncDataToken dataToken = getQueryExecutor().executeAsync(groupRequest);
        Thread.sleep(1000);

        try {
            client().resource(String.format("/v1/async/distinct/%s", dataToken.getKey())).type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetResponseInvalidKey() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        AsyncDataToken dataToken = getQueryExecutor().executeAsync(groupRequest);
        Thread.sleep(1000);

        try {
            client().resource(String.format("/v1/async/%s/dummy", dataToken.getAction())).type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetResponsePost() throws Exception {
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

        AsyncDataToken dataToken = getQueryExecutor().executeAsync(groupRequest);
        Thread.sleep(1000);

        GroupResponse response = client().resource("/v1/async")
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(GroupResponse.class, dataToken);

        assertEquals(expectedResponse, response.getResult());
    }

    // TODO Not sure if returning no content is correct
    @Test
    public void testGetResponsePostInvalidKey() throws Exception {
        AsyncDataToken dataToken = new AsyncDataToken("group", null);
        GroupResponse response = client().resource("/v1/async").type(MediaType.APPLICATION_JSON_TYPE).post(GroupResponse.class, dataToken);
        assertNull(response);
    }

    @Test
    public void testGetResponsePostInvalidAction() throws Exception {
        AsyncDataToken dataToken = new AsyncDataToken(null, UUID.randomUUID().toString());
        try {
            client().resource("/v1/async")
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(GroupResponse.class, dataToken);
            fail();
        } catch (NullPointerException ex) {
        }
    }
}
