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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AnalyticsResourceTest {
    //    private TableMetadataManager tableMetadataManager;
//    private MockElasticsearchServer elasticsearchServer;
//    private HazelcastInstance hazelcastInstance;
    private static final QueryExecutor queryExecutor = mock(QueryExecutor.class);

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        mapper.setSubtypeResolver(subtypeResolver);
        TestUtils.registerSubTypes(mapper, ActionRequest.class);
    }

    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder().setMapper(mapper).addResource(new AnalyticsResource(queryExecutor)).build();

    @Before
    public void setUp() throws Exception {
        reset(queryExecutor);
    }

    @Test
    public void testRunSync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        doReturn(new GroupResponse()).when(queryExecutor).execute(any(GroupRequest.class));

        Response response = resource.client().target("/v1/analytics").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(groupRequest));
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE, "Dummy Message")).when(queryExecutor).execute(any(GroupRequest.class));

        Response response = resource.client().target("/v1/analytics").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(groupRequest));
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRunAsync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        doReturn(new AsyncDataToken()).when(queryExecutor).executeAsync(any(GroupRequest.class));

        Response response = resource.client().target("/v1/analytics/async").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(groupRequest));
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRunAsyncError() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DATA_CLEANUP_ERROR, "Dummy Error")).when(queryExecutor).executeAsync(any(GroupRequest.class));

        Response response = resource.client().target("/v1/analytics/async").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(groupRequest));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }
}
