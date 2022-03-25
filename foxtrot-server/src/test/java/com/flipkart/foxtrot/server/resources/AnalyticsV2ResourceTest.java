package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.console.QueryManager;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class AnalyticsV2ResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    private QueryManager queryManager;

    public AnalyticsV2ResourceTest() throws IOException {
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().saveAll(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchConnection.getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        queryManager = mock(QueryManager.class);
        resources = ResourceTestRule.builder()
                .addResource(new AnalyticsV2Resource(getQueryExecutorFactory(), getObjectMapper(),
                        new QueryConfig(), queryManager))
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .setMapper(objectMapper)
                .build();

    }


    @Test
    public void testRunSync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
        groupRequest.setSourceType(SourceType.ECHO_BROWSE_EVENTS);

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
        GroupResponse response = resources.target("/v2/analytics")
                .request()
                .post(serviceUserEntity, GroupResponse.class);
        assertEquals(expectedResponse, response.getResult());
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
        groupRequest.setSourceType(SourceType.ECHO_BROWSE_EVENTS);

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
        groupRequest.setSourceType(SourceType.ECHO_BROWSE_EVENTS);

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
        AsyncDataToken response = resources.target("/v2/analytics/async")
                .request()
                .post(serviceUserEntity, AsyncDataToken.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        GroupResponse actualResponse = GroupResponse.class.cast(getCacheManager().getCacheFor(response.getAction())
                .get(response.getKey()));
        assertEquals(expectedResponse, actualResponse.getResult());
    }

    @Test
    public void testRunSyncAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
        groupRequest.setSourceType(SourceType.ECHO_BROWSE_EVENTS);

        GroupResponse expectedResponse = new GroupResponse();
        Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
        AsyncDataToken asyncDataToken = resources.target("/v2/analytics/async")
                .request()
                .header("Authorization", "Bearer TOKEN")
                .post(serviceUserEntity, AsyncDataToken.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        GroupResponse actualResponse = GroupResponse.class.cast(
                getCacheManager().getCacheFor(asyncDataToken.getAction())
                        .get(asyncDataToken.getKey()));
        assertEquals(expectedResponse.getResult(), actualResponse.getResult());
    }

    @Test
    public void testValidate() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));
        groupRequest.setSourceType(SourceType.ECHO_BROWSE_EVENTS);

        Entity<GroupRequest> serviceUserEntity = Entity.json(groupRequest);
        ActionValidationResponse response = resources.target("/v2/analytics/validate")
                .request()
                .post(serviceUserEntity, ActionValidationResponse.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        GroupRequest actionValidationRequest = (GroupRequest) response.getProcessedRequest();
        assertEquals(actionValidationRequest.getOpcode(), groupRequest.getOpcode());
        assertEquals(actionValidationRequest.getTable(), groupRequest.getTable());
        assertEquals(actionValidationRequest.getAggregationField(), groupRequest.getAggregationField());
        assertEquals(actionValidationRequest.getAggregationType(), groupRequest.getAggregationType());
        assertEquals(actionValidationRequest.getConsoleId(), groupRequest.getConsoleId());
        assertEquals(actionValidationRequest.getNesting(), groupRequest.getNesting());
        assertEquals(actionValidationRequest.getUniqueCountOn(), groupRequest.getUniqueCountOn());
        assertNull(response.getValidationErrors());
    }

}
