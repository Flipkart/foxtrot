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

import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
@RunWith(PowerMockRunner.class)
public class AsyncResourceTest {
    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder().addResource(new AsyncResource()).build();

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

    public void setUp() throws Exception {

    }

    @PrepareForTest(CacheUtils.class)
    @Ignore
    @Test
    public void testGetResponse() throws Exception {
        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        AsyncDataToken dataToken = new AsyncDataToken("action", "key");

        GroupResponse groupResponse = mock(GroupResponse.class);
        doReturn(expectedResponse).when(groupResponse).getResult();

        mockStatic(CacheUtils.class);
        Cache cache = mock(Cache.class);
        doReturn(groupResponse).when(cache).get(dataToken.getKey());
        when(CacheUtils.getCacheFor(dataToken.getAction())).thenReturn(cache);
//
        Response response = resource.client().target("/v1/async/" + dataToken.getAction() + "/" + dataToken.getKey()).request().get();

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

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
}
