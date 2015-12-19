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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.StoreExecutionException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.api.container.MappableContainerException;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class DocumentResourceTest extends ResourceTest {
    private ObjectMapper mapper = new ObjectMapper();
    private JsonNodeFactory factory = JsonNodeFactory.instance;

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;

    private QueryStore queryStore;

    public DocumentResourceTest() throws Exception {

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
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        queryStore = spy(queryStore);
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new DocumentResource(queryStore));
        addProvider(FoxtrotExceptionMapper.class);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }


    @Test
    public void testSaveDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(
                id,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post(document);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document response = queryStore.get(TestUtils.TEST_TABLE_NAME, id);
        compare(document, response);
    }

    @Test
    public void testSaveDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(
                id,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        doThrow(new StoreExecutionException("dummy", new IOException()))
                .when(queryStore).save(anyString(), Matchers.<Document>any());
        try {
            client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post(document);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentNullId() throws Exception {
        Document document = new Document(
                null,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post(document);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentNullData() throws Exception {
        Document document = new Document(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                null);
        client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post(document);
    }

    @Test(expected = ContainerException.class)
    public void testSaveDocumentUnknownObject() throws Exception {
        client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post("hello");
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentEmptyJson() throws Exception {
        client().resource("/v1/document/" + TestUtils.TEST_TABLE_NAME).type(MediaType.APPLICATION_JSON_TYPE).post("{}");
    }


    @Test
    public void testSaveDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).type(MediaType.APPLICATION_JSON_TYPE).post(documents);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        compare(document1, queryStore.get(TestUtils.TEST_TABLE_NAME, id1));
        compare(document2, queryStore.get(TestUtils.TEST_TABLE_NAME, id2));
    }

    @Test
    public void testSaveDocumentsInternalError() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        doThrow(new StoreExecutionException("dummy", new IOException()))
                .when(queryStore).save(anyString(), anyListOf(Document.class));
        try {
            client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).type(MediaType.APPLICATION_JSON_TYPE).post(documents);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentsNullDocuments() throws Exception {
        List<Document> documents = null;
        client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).type(MediaType.APPLICATION_JSON_TYPE).post(documents);
    }

    @Test
    public void testSaveDocumentsNullDocument() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(null);
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), factory.objectNode().put("d", "d")));
        try {
            client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testSaveDocumentsNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(null, System.currentTimeMillis(), factory.objectNode().put("d", "d")));
        try {
            client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testSaveDocumentsNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
        try {
            client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test(expected = MappableContainerException.class)
    public void testSaveDocumentsInvalidRequestObject() throws Exception {
        client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post("Hello");
    }

    @Test
    public void testSaveDocumentsEmptyList() throws Exception {
        try {
            client().resource(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post("[]");
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        queryStore.save(TestUtils.TEST_TABLE_NAME, document);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document response = client().resource(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id))
                .get(Document.class);
        compare(document, response);
    }

    @Test
    public void testGetDocumentMissingId() throws Exception {
        String id = UUID.randomUUID().toString();
        try {
            client().resource(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id))
                    .get(Document.class);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        try {
            doThrow(new StoreExecutionException("dummy", new IOException()))
                    .when(queryStore).get(anyString(), anyString());
            client().resource(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id))
                    .get(Document.class);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }


    @Test
    public void testGetDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        String response = client().resource(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                .queryParam("id", id1)
                .queryParam("id", id2)
                .get(String.class);
        String expectedResponse = mapper.writeValueAsString(documents);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testGetDocumentsNoIds() throws Exception {
        String response = client().resource(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                .get(String.class);
        String expectedResponse = mapper.writeValueAsString(new ArrayList<Document>());
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testGetDocumentsMissingIds() throws Exception {
        try {
            client().resource(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                    .queryParam("id", UUID.randomUUID().toString())
                    .get(String.class);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetDocumentsInternalError() throws Exception {
        try {
            doThrow(new StoreExecutionException("dummy", new IOException()))
                    .when(queryStore).getAll(anyString(), anyListOf(String.class));
            client().resource(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                    .queryParam("id", UUID.randomUUID().toString())
                    .get(String.class);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }


    public void compare(Document expected, Document actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
        Map<String, Object> expectedMap = mapper.convertValue(expected.getData(), new TypeReference<HashMap<String, Object>>() {
        });
        Map<String, Object> actualMap = mapper.convertValue(actual.getData(), new TypeReference<HashMap<String, Object>>() {
        });
        assertEquals("Actual data should match expected data", expectedMap, actualMap);
    }
}
