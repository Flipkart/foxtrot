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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class DocumentResourceTest {
    private JsonNodeFactory factory = JsonNodeFactory.instance;
    private static QueryStore queryStore = mock(QueryStore.class);

    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder().addResource(new DocumentResource(queryStore)).build();

    @Before
    public void setUp() throws Exception {
        reset(queryStore);
    }

    @Test
    public void testSaveDocument() throws Exception {
        Document document = new Document("1", System.currentTimeMillis(), null);
        doNothing().when(queryStore).save(eq(TestUtils.TEST_TABLE_NAME), eq(document));
        Response response = resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(document));
        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("hello", "world"));
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR, "Dummy Exception"))
                .when(queryStore).save(anyString(), any(Document.class));

        Response response = resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(document));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentNoSuchTableError() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("hello", "world"));
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE, "Dummy Exception"))
                .when(queryStore).save(anyString(), any(Document.class));

        Response response = resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(document));
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentInvalidRequestError() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("hello", "world"));
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Dummy Exception"))
                .when(queryStore).save(anyString(), any(Document.class));

        Response response = resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(document));
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentUnknownObject() throws Exception {
        resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json("hello"));
    }

    @Test
    public void testSaveDocumentEmptyJson() throws Exception {
        Response response = resource.client().target("/v1/document/" + TestUtils.TEST_TABLE_NAME).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json("{}"));
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
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
        doNothing().when(queryStore).save(anyString(), anyListOf(Document.class));
        Response response = resource.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(documents));
        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
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
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR, "Dummy Exception"))
                .when(queryStore).save(anyString(), anyListOf(Document.class));

        Response response = resource.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(documents));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentsInvalidRequestObject() throws Exception {
        resource.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME)).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json("Hello"));
    }

    @Test
    public void testGetDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        when(queryStore.get(anyString(), anyString())).thenReturn(document);

        Response response = resource.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id)).request().get();
        Document responseEntity = response.readEntity(Document.class);
        assertEquals(document.getId(), responseEntity.getId());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetDocumentMissingId() throws Exception {
        String id = UUID.randomUUID().toString();
        when(queryStore.get(eq(TestUtils.TEST_TABLE_NAME), eq(id))).thenThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, "Dummy"));

        Response response = resource.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id)).request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR, "Error"))
                    .when(queryStore).get(anyString(), anyString());
        Response response = resource.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id)).request().get();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
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

        when(queryStore.getAll(eq(TestUtils.TEST_TABLE_NAME), eq(Arrays.asList(id1, id2)))).thenReturn(documents);

        Response response = resource.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                .queryParam("id", id1)
                .queryParam("id", id2)
                .request().get();

        List<Document> responseEntity = response.readEntity(List.class);
        assertEquals(documents.size(), responseEntity.size());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetDocumentsMissingIds() throws Exception {
        when(queryStore.getAll(eq(TestUtils.TEST_TABLE_NAME), anyList())).thenThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, "Dummy"));
        Response response = resource.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                    .queryParam("id", UUID.randomUUID().toString())
                    .request().get();

        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetDocumentsInternalError() throws Exception {
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR, "Error"))
                    .when(queryStore).getAll(anyString(), anyListOf(String.class));

        Response response = resource.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                    .queryParam("id", UUID.randomUUID().toString())
                    .request().get();

        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }
}
