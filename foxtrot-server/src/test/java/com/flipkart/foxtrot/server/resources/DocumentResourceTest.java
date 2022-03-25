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

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import com.foxtrot.flipkart.translator.TableTranslator;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class DocumentResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources = ResourceTestUtils.testResourceBuilder(getMapper())
            .addResource(new DocumentResource(getQueryStore(), new TableTranslator(new SegregationConfiguration())))
            .build();

    public DocumentResourceTest() throws IOException {
    }

    @Test
    public void testSaveDocument() throws Exception {
        String id = UUID.randomUUID()
                .toString();
        Document document = new Document(id, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("hello", "world"));
        Entity<Document> documentEntity = Entity.json(document);
        resources.target("/v1/document/" + TestUtils.TEST_TABLE_NAME)
                .request()
                .post(documentEntity);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document response = getQueryStore().get(TestUtils.TEST_TABLE_NAME, id);
        compare(document, response);
    }

    @Test
    public void testSaveDocumentNullId() throws Exception {
        Document document = new Document(null, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("hello", "world"));
        Entity<Document> documentEntity = Entity.json(document);
        Response response = resources.target("/v1/document/" + TestUtils.TEST_TABLE_NAME)
                .request()
                .post(documentEntity);
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testSaveDocumentNullData() throws Exception {
        Document document = new Document(UUID.randomUUID()
                .toString(), System.currentTimeMillis(), null);
        Entity<Document> documentEntity = Entity.json(document);
        Response response = resources.target("/v1/document/" + TestUtils.TEST_TABLE_NAME)
                .request()
                .post(documentEntity);
        assertEquals(422, response.getStatus());
    }

    @Test
    public void testSaveDocumentUnknownObject() throws Exception {
        Response response = resources.target("/v1/document/" + TestUtils.TEST_TABLE_NAME)
                .request()
                .post(Entity.text("hello"));
        assertEquals(Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentEmptyJson() throws Exception {
        Response response = resources.target("/v1/document/" + TestUtils.TEST_TABLE_NAME)
                .request()
                .post(Entity.json("{}"));
        assertEquals(422, response.getStatus());
    }

    @Test
    public void testSaveDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID()
                .toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("D", "data"));
        String id2 = UUID.randomUUID()
                .toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        Entity<List<Document>> listEntity = Entity.json(documents);
        resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(listEntity);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        compare(document1, getQueryStore().get(TestUtils.TEST_TABLE_NAME, id1));
        compare(document2, getQueryStore().get(TestUtils.TEST_TABLE_NAME, id2));
    }

    @Test
    public void testSaveDocumentsNullDocuments() throws Exception {
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(null);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentsNullDocument() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(null);
        documents.add(new Document(UUID.randomUUID()
                .toString(), System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("d", "d")));
        Entity<List<Document>> listEntity = Entity.json(documents);
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(listEntity);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentsNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(null, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("d", "d")));
        Entity<List<Document>> listEntity = Entity.json(documents);
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(listEntity);
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testSaveDocumentsNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID()
                .toString(), System.currentTimeMillis(), null));
        Entity<List<Document>> listEntity = Entity.json(documents);
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(listEntity);
        assertEquals(422, response.getStatus());
    }

    @Test
    public void testSaveDocumentsInvalidRequestObject() throws Exception {
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(Entity.text("Hello"));
        assertEquals(Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveDocumentsEmptyList() throws Exception {
        Entity<List<Document>> list = Entity.json(Collections.emptyList());
        Response response = resources.target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE_NAME))
                .request()
                .post(list);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetDocument() throws Exception {
        String id = UUID.randomUUID()
                .toString();
        Document document = new Document(id, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("D", "data"));
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, document);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document response = resources.target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id))
                .request()
                .get(Document.class);
        compare(document, response);
    }

    @Test
    public void testGetDocumentMissingId() throws Exception {
        String id = UUID.randomUUID()
                .toString();
        try {
            resources.target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE_NAME, id))
                    .request()
                    .get(Document.class);
            fail();
        } catch (WebApplicationException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse()
                    .getStatus());
        }
    }

    @Test
    public void testGetDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID()
                .toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("D", "data"));
        String id2 = UUID.randomUUID()
                .toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), getMapper().getNodeFactory()
                .objectNode()
                .put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        getQueryStore().saveAll(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        String response = resources.target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                .queryParam("id", id1)
                .queryParam("id", id2)
                .request()
                .get(String.class);
        String expectedResponse = getMapper().writeValueAsString(documents);
        assertEquals(expectedResponse, response);
    }

    @Test(expected = BadRequestException.class)
    public void testGetDocumentsNoIds() throws Exception {
        String response = resources.target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                .request()
                .get(String.class);
    }

    @Test
    public void testGetDocumentsMissingIds() throws Exception {
        try {
            resources.target(String.format("/v1/document/%s", TestUtils.TEST_TABLE_NAME))
                    .queryParam("id", UUID.randomUUID()
                            .toString())
                    .request()
                    .get(String.class);
            fail();
        } catch (WebApplicationException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse()
                    .getStatus());
        }
    }

    public void compare(Document expected,
                        Document actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(),
                actual.getTimestamp());
        Map<String, Object> expectedMap = getMapper().convertValue(expected.getData(),
                new TypeReference<HashMap<String, Object>>() {
                });
        Map<String, Object> actualMap = getMapper().convertValue(actual.getData(),
                new TypeReference<HashMap<String, Object>>() {
                });
        assertEquals("Actual data should match expected data", expectedMap, actualMap);
    }
}
