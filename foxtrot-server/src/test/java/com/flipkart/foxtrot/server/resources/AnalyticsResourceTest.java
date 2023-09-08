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
package com.flipkart.foxtrot.server.resources;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.client.RequestOptions;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AnalyticsResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    public AnalyticsResourceTest() throws Exception {
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getOpensearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new AnalyticsResource(getQueryExecutor(), objectMapper))
                .build();
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

        Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
        GroupResponse response = resources.target("/v1/analytics")
                .request()
                .post(serviceUserEntity, GroupResponse.class);
        assertEquals(expectedResponse, response.getResult());
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        try {
            Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
            resources.target("/v1/generate/test")
                    .request()
                    .post(serviceUserEntity, GroupResponse.class);
            fail();
        } catch (WebApplicationException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse()
                    .getStatus());
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
        Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
        AsyncDataToken response = resources.target("/v1/analytics/async")
                .request()
                .post(serviceUserEntity, AsyncDataToken.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS).until(() -> true);
        GroupResponse actualResponse = GroupResponse.class.cast(getCacheManager().getCacheFor(response.getAction())
                .get(response.getKey()));
        assertEquals(expectedResponse, actualResponse.getResult());
    }

    @Test
    public void testRunSyncAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GroupResponse expectedResponse = new GroupResponse();
        Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
        AsyncDataToken asyncDataToken = resources.target("/v1/analytics/async")
                .request()
                .header("Authorization", "Bearer TOKEN")
                .post(serviceUserEntity, AsyncDataToken.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS).until(() -> true);
        GroupResponse actualResponse = GroupResponse.class.cast(getCacheManager().getCacheFor(asyncDataToken.getAction())
                .get(asyncDataToken.getKey()));
        assertEquals(expectedResponse.getResult(), actualResponse.getResult());
    }
}
