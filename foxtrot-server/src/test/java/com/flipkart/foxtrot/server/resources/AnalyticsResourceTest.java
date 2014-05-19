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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.testing.ResourceTest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AnalyticsResourceTest extends ResourceTest {

    private ObjectMapper mapper;
    private JsonNodeFactory factory = JsonNodeFactory.instance;

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private QueryExecutor queryExecutor;

    public AnalyticsResourceTest() throws Exception {
        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        getObjectMapperFactory().setSubtypeResolver(subtypeResolver);

        mapper = getObjectMapperFactory().build();
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        tableMetadataManager.start();
        when(tableMetadataManager.exists(anyString())).thenReturn(true);


        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor)
                .save(TestUtils.TEST_TABLE, TestUtils.getGroupDocuments(mapper));
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new AnalyticsResource(queryExecutor));
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }

    @Test
    public void testRunSync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2).put("1", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("2", 1));
        temp.put("ipad", factory.objectNode().put("2", 2));
        temp.put("iphone", factory.objectNode().put("1", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        WebResource webResource = client().resource("/v1/analytics");
        GroupResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(GroupResponse.class, groupRequest);
        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(response);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE + "-dummy");
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
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2).put("1", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("2", 1));
        temp.put("ipad", factory.objectNode().put("2", 2));
        temp.put("iphone", factory.objectNode().put("1", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        WebResource webResource = client().resource("/v1/analytics/async");
        AsyncDataToken response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(AsyncDataToken.class, groupRequest);
        Thread.sleep(2000);
        ActionResponse completeResponse = CacheUtils.getCacheFor(response.getAction()).get(response.getKey());

        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(completeResponse);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testRunSyncAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        WebResource webResource = client().resource("/v1/analytics/async");
        AsyncDataToken asyncDataToken = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(AsyncDataToken.class, groupRequest);
        Thread.sleep(2000);
        ActionResponse response = CacheUtils.getCacheFor(asyncDataToken.getAction()).get(asyncDataToken.getKey());
        assertEquals(finalNode.toString(), mapper.writeValueAsString(response));
    }
}
