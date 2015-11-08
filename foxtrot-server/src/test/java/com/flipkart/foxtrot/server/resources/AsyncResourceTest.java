///**
// * Copyright 2014 Flipkart Internet Pvt. Ltd.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.flipkart.foxtrot.server.resources;
//
//import com.fasterxml.jackson.annotation.JsonInclude;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
//import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
//import com.flipkart.foxtrot.common.Document;
//import com.flipkart.foxtrot.common.group.GroupRequest;
//import com.flipkart.foxtrot.common.group.GroupResponse;
//import com.flipkart.foxtrot.core.MockElasticsearchServer;
//import com.flipkart.foxtrot.core.TestUtils;
//import com.flipkart.foxtrot.core.common.AsyncDataToken;
//import com.flipkart.foxtrot.core.common.CacheUtils;
//import com.flipkart.foxtrot.core.datastore.DataStore;
//import com.flipkart.foxtrot.core.querystore.QueryExecutor;
//import com.flipkart.foxtrot.core.querystore.QueryStore;
//import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
//import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
//import com.flipkart.foxtrot.core.querystore.impl.*;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.test.TestHazelcastInstanceFactory;
//import com.sun.jersey.api.client.UniformInterfaceException;
//import com.sun.jersey.api.client.WebResource;
//import com.yammer.dropwizard.testing.ResourceTest;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.common.settings.Settings;
//import org.junit.After;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response;
//import java.util.*;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Mockito.when;
//
///**
// * Created by rishabh.goyal on 05/05/14.
// */
//public class AsyncResourceTest extends ResourceTest {
//
//    private TableMetadataManager tableMetadataManager;
//    private MockElasticsearchServer elasticsearchServer;
//    private HazelcastInstance hazelcastInstance;
//    private QueryExecutor queryExecutor;
//
//    public AsyncResourceTest() throws Exception {
//        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
//        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
//        getObjectMapperFactory().setSubtypeResolver(subtypeResolver);
//
//        ObjectMapper mapper = getObjectMapperFactory().build();
//        ElasticsearchUtils.setMapper(mapper);
//        DataStore dataStore = TestUtils.getDataStore();
//
//        //Initializing Cache Factory
//        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
//        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
//        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
//        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));
//
//        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
//        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
//        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
//        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());
//
//        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
//        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
//        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
//        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
//
//        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
//        tableMetadataManager.start();
//        when(tableMetadataManager.exists(anyString())).thenReturn(true);
//        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
//
//        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
//
//        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
//        TestUtils.registerActions(analyticsLoader, mapper);
//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
//        List<Document> documents = TestUtils.getGroupDocuments(mapper);
//        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
//        for (Document document : documents) {
//            elasticsearchServer.getClient().admin().indices()
//                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
//                    .setForce(true).execute().actionGet();
//        }
//    }
//
//
//    @Override
//    protected void setUpResources() throws Exception {
//        addResource(new AsyncResource());
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        elasticsearchServer.shutdown();
//        hazelcastInstance.shutdown();
//        tableMetadataManager.stop();
//    }
//
//    @Test
//    public void testGetResponse() throws Exception {
//        GroupRequest groupRequest = new GroupRequest();
//        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
//        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
//
//        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();
//
//        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
//        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
//        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
//            put("nexus", nexusResponse);
//            put("galaxy", galaxyResponse);
//        }});
//
//        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
//        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
//        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
//        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
//            put("nexus", nexusResponse2);
//            put("ipad", iPadResponse);
//            put("iphone", iPhoneResponse);
//        }});
//
//        AsyncDataToken dataToken = queryExecutor.executeAsync(groupRequest);
//        Thread.sleep(1000);
//
//        WebResource webResource = client().resource("/v1/async/" + dataToken.getAction() + "/" + dataToken.getKey());
//        GroupResponse groupResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);
//
//        assertEquals(expectedResponse, groupResponse.getResult());
//    }
//
//    @Test
//    public void testGetResponseInvalidAction() throws Exception {
//        GroupRequest groupRequest = new GroupRequest();
//        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
//        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
//
//        AsyncDataToken dataToken = queryExecutor.executeAsync(groupRequest);
//        Thread.sleep(1000);
//
//        try {
//            client().resource(String.format("/v1/async/distinct/%s", dataToken.getKey())).type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);
//        } catch (UniformInterfaceException ex) {
//            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
//        }
//    }
//
//    @Test
//    public void testGetResponseInvalidKey() throws Exception {
//        GroupRequest groupRequest = new GroupRequest();
//        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
//        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
//
//        AsyncDataToken dataToken = queryExecutor.executeAsync(groupRequest);
//        Thread.sleep(1000);
//
//        try {
//            client().resource(String.format("/v1/async/%s/dummy", dataToken.getAction())).type(MediaType.APPLICATION_JSON_TYPE).get(GroupResponse.class);
//        } catch (UniformInterfaceException ex) {
//            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
//        }
//    }
//
//    @Test
//    public void testGetResponsePost() throws Exception {
//        GroupRequest groupRequest = new GroupRequest();
//        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
//        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
//
//        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();
//
//        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
//        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
//        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
//            put("nexus", nexusResponse);
//            put("galaxy", galaxyResponse);
//        }});
//
//        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
//        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
//        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
//        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
//            put("nexus", nexusResponse2);
//            put("ipad", iPadResponse);
//            put("iphone", iPhoneResponse);
//        }});
//
//        AsyncDataToken dataToken = queryExecutor.executeAsync(groupRequest);
//        Thread.sleep(1000);
//
//        GroupResponse response = client().resource("/v1/async")
//                .type(MediaType.APPLICATION_JSON_TYPE)
//                .post(GroupResponse.class, dataToken);
//
//        assertEquals(expectedResponse, response.getResult());
//    }
//
//    // TODO Not sure if returning no content is correct
//    @Test
//    public void testGetResponsePostInvalidKey() throws Exception {
//        AsyncDataToken dataToken = new AsyncDataToken("group", null);
//        GroupResponse response = client().resource("/v1/async").type(MediaType.APPLICATION_JSON_TYPE).post(GroupResponse.class, dataToken);
//        assertNull(response);
//    }
//
//    @Test
//    public void testGetResponsePostInvalidAction() throws Exception {
//        AsyncDataToken dataToken = new AsyncDataToken(null, UUID.randomUUID().toString());
//
//        try {
//            client().resource("/v1/async")
//                    .type(MediaType.APPLICATION_JSON_TYPE)
//                    .post(GroupResponse.class, dataToken);
//        } catch (UniformInterfaceException ex) {
//            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
//        }
//    }
//}
