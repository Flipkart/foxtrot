package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.common.FqlRequest;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.exception.FqlParsingException;
import com.flipkart.foxtrot.common.headers.FoxtrotRequestInfoHeaders;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.console.QueryManager;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlGetRequest;
import com.flipkart.foxtrot.sql.fqlstore.FqlStore;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mortbay.log.Log;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class FqlV2ResourceTest extends FoxtrotResourceTest {

    private QueryManager queryManager = mock(QueryManager.class);
    @Rule
    public ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new FqlV2Resource(
                    new FqlEngine(getTableMetadataManager(), getQueryStore(), getQueryExecutorFactory(), getMapper()),
                    new FqlStoreServiceImpl(elasticsearchConnection, getMapper()),
                    new QueryConfig(), queryManager))
            .addProvider(new FoxtrotExceptionMapper(getMapper()))
            .setMapper(objectMapper)
            .build();


    public FqlV2ResourceTest() throws IOException {
        doNothing().when(queryManager)
                .checkIfQueryAllowed(any(String.class), any(SourceType.class));
    }

    @Test
    public void testExecuteQuery() throws Exception {
        String query = "show tables";
        Entity<String> stringEntity = Entity.json(query);
        FlatRepresentation flatRepresentation = resources.client()
                .target("/v2/fql")
                .request()
                .header(FoxtrotRequestInfoHeaders.SOURCE_TYPE, SourceType.FQL.name())
                .post(stringEntity, FlatRepresentation.class);

        Assert.assertNotNull(flatRepresentation);
        Assert.assertEquals(2, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("name", flatRepresentation.getHeaders()
                .get(0)
                .getName());
        Assert.assertEquals("ttl", flatRepresentation.getHeaders()
                .get(1)
                .getName());
        Assert.assertEquals(1, flatRepresentation.getRows()
                .size());
        Assert.assertEquals("test-table", flatRepresentation.getRows()
                .get(0)
                .get("name"));
        Assert.assertEquals(7, flatRepresentation.getRows()
                .get(0)
                .get("ttl"));
        Assert.assertEquals(4, flatRepresentation.getRows()
                .get(0)
                .get("defaultRegions"));
    }

    @Test
    public void testExecuteQueryParseFail() {
        String query = "select * from test1 where";
        Entity<String> stringEntity = Entity.json(query);
        Response response = resources.client()
                .target("/v2/fql")
                .request()
                .header(FoxtrotRequestInfoHeaders.SOURCE_TYPE, SourceType.FQL.name())
                .post(stringEntity);
        Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        FoxtrotException fqlParseError = response.readEntity(FqlParsingException.class);
        Assert.assertEquals(ErrorCode.FQL_PARSE_ERROR, fqlParseError.getCode());
    }

    @Test
    public void testNullResponseFromExecuteQuery() {
        String query = "select * from test1";
        Entity<String> stringEntity = Entity.json(query);
        Response response = resources.client()
                .target("/v2/fql")
                .request()
                .header(FoxtrotRequestInfoHeaders.SOURCE_TYPE, SourceType.FQL.name())
                .post(stringEntity);
        Assert.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveFqlStore() {
        String title = "testQuery";
        String query = "select * from test";

        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle(title);
        fqlStore.setQuery(query);
        Entity<FqlStore> fqlStoreEntity = Entity.json(fqlStore);
        FqlStore fqlStoreResponse = resources.client()
                .target("/v2/fql/save")
                .request()
                .post(fqlStoreEntity, FqlStore.class);

        Assert.assertNotNull(fqlStoreResponse);
        Assert.assertEquals(title, fqlStoreResponse.getTitle());
        Assert.assertEquals(query, fqlStoreResponse.getQuery());
    }

    @Test
    public void testFqlQueryWithExtrapolation() {
        String query = "select * from test1";
        Entity<FqlRequest> fqlRequestEntity = Entity.json(new FqlRequest(query, true));
        Response response = resources.client()
                .target("/v2/fql/extrapolation")
                .request()
                .post(fqlRequestEntity);
        Assert.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());


    }

    @Test
    public void testGetSavedFqlStore() throws InterruptedException {
        String title = "title1";
        String query = "show tables";

        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle(title);
        fqlStore.setQuery(query);
        fqlStore.setUserId("userId");
        Entity<FqlStore> fqlStoreEntity = Entity.json(fqlStore);
        FqlStore fqlStoreResponse = resources.client()
                .target("/v2/fql/save")
                .request()
                .post(fqlStoreEntity, FqlStore.class);
        Assert.assertNotNull(fqlStoreResponse);
        Assert.assertEquals(title, fqlStoreResponse.getTitle());
        Assert.assertEquals(query, fqlStoreResponse.getQuery());

        TimeUnit.SECONDS.sleep(1);

        FqlGetRequest fqlGetRequest = new FqlGetRequest();
        fqlGetRequest.setTitle(title);
        fqlGetRequest.setUserId("userId");
        Entity<FqlGetRequest> fqlGetRequestEntity = Entity.json(fqlGetRequest);
        List<FqlStore> result = getMapper().convertValue(resources.client()
                .target("/v2/fql/get")
                .request()
                .post(fqlGetRequestEntity, List.class), new TypeReference<List<FqlStore>>() {
        });
        Log.debug("list<FqlStore> got is : {}", result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(title, result.get(0)
                .getTitle());
        Assert.assertEquals(query, result.get(0)
                .getQuery());
    }
}
