package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.container.MappableContainerException;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class DocumentResourceTest extends ResourceTest {
    private final Logger logger = LoggerFactory.getLogger(DocumentResourceTest.class.getSimpleName());
    private ObjectMapper mapper = new ObjectMapper();
    private JsonNodeFactory factory = JsonNodeFactory.instance;
    private QueryStore queryStore;

    public DocumentResourceTest() throws Exception {
        logger.info("In Setup");
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        // Ensure that table exists before saving/reading data from it
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE)).thenReturn(true);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        QueryExecutor queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor);
    }

    @Override
    protected void setUpResources() throws Exception {
        logger.info("Setting Resources");
        addResource(new DocumentResource(queryStore));
    }


    @Test
    public void testSaveDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(
                id,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        client().resource("/foxtrot/v1/document/" + TestUtils.TEST_TABLE).type(MediaType.APPLICATION_JSON_TYPE).post(document);
        Document response = queryStore.get(TestUtils.TEST_TABLE, id);
        compare(document, response);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentNullId() throws Exception {
        Document document = new Document(
                null,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        client().resource("/foxtrot/v1/document/" + TestUtils.TEST_TABLE).type(MediaType.APPLICATION_JSON_TYPE).post(document);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentNullData() throws Exception {
        Document document = new Document(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                null);
        client().resource("/foxtrot/v1/document/" + TestUtils.TEST_TABLE).type(MediaType.APPLICATION_JSON_TYPE).post(document);
    }

    @Test(expected = MappableContainerException.class)
    public void testSaveDocumentUnknownObject() throws Exception {
        client().resource("/foxtrot/v1/document/" + TestUtils.TEST_TABLE).type(MediaType.APPLICATION_JSON_TYPE).post("hello");
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentEmptyJson() throws Exception {
        client().resource("/foxtrot/v1/document/" + TestUtils.TEST_TABLE).type(MediaType.APPLICATION_JSON_TYPE).post("{}");
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
        client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE)).type(MediaType.APPLICATION_JSON_TYPE).post(documents);

        compare(document1, queryStore.get(TestUtils.TEST_TABLE, id1));
        compare(document2, queryStore.get(TestUtils.TEST_TABLE, id2));
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveDocumentsNullDocuments() throws Exception {
        List<Document> documents = null;
        client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE)).type(MediaType.APPLICATION_JSON_TYPE).post(documents);
    }

    @Test
    public void testSaveDocumentsNullDocument() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(null);
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), factory.objectNode().put("d", "d")));
        try {
            client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
        }
    }

    @Test
    public void testSaveDocumentsNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(null, System.currentTimeMillis(), factory.objectNode().put("d", "d")));
        try {
            client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
        }
    }

    @Test
    public void testSaveDocumentsNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
        try {
            client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post(documents);
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
        }
    }

    @Test(expected = MappableContainerException.class)
    public void testSaveDocumentsInvalidRequestObject() throws Exception {
        client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post("Hello");
    }

    @Test
    public void testSaveDocumentsEmptyList() throws Exception {
        try {
            client().resource(String.format("/foxtrot/v1/document/%s/bulk", TestUtils.TEST_TABLE))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post("[]");
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
        }
    }


    @Test
    public void testGetDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        queryStore.save(TestUtils.TEST_TABLE, document);

        Document response = client().resource(String.format("/foxtrot/v1/document/%s/%s", TestUtils.TEST_TABLE, id))
                .get(Document.class);
        compare(document, response);
    }

    @Test
    public void testGetDocumentMissingId() throws Exception {
        String id = UUID.randomUUID().toString();
        try {
            client().resource(String.format("/foxtrot/v1/document/%s/%s", TestUtils.TEST_TABLE, id))
                    .get(Document.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
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
        queryStore.save(TestUtils.TEST_TABLE, documents);
        String response = client().resource(String.format("/foxtrot/v1/document/%s", TestUtils.TEST_TABLE))
                .queryParam("id", id1)
                .queryParam("id", id2)
                .get(String.class);

        String expectedResponse = mapper.writeValueAsString(documents);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testGetDocumentsNoIds() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        queryStore.save(TestUtils.TEST_TABLE, documents);
        String response = client().resource(String.format("/foxtrot/v1/document/%s", TestUtils.TEST_TABLE))
                .get(String.class);
        String expectedResponse = mapper.writeValueAsString(new ArrayList<Document>());
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testGetDocumentsMissingIds() throws Exception {
        try {
            client().resource(String.format("/foxtrot/v1/document/%s", TestUtils.TEST_TABLE))
                    .queryParam("id", UUID.randomUUID().toString())
                    .get(String.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(500, ex.getResponse().getStatus());
        }
    }


    public void compare(Document expected, Document actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
        String expectedData = mapper.writeValueAsString(expected.getData());
        String actualData = mapper.writeValueAsString(actual.getData());
        assertEquals("Actual data should match expected data", expectedData, actualData);
    }
}
