package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.*;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutor;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutorFactory;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class FqlEngineTest {

    private FqlEngine fqlEngine;
    private QueryExecutor queryExecutor;
    private TableMetadataManager tableMetadataManager;
    private QueryStore queryStore;

    @Before
    public void setUp() throws Exception {
        QueryExecutorFactory queryExecutorFactory = Mockito.mock(QueryExecutorFactory.class);
        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        Mockito.when(tableMetadataManager.get())
                .thenReturn(new ArrayList<Table>());
        queryStore = Mockito.mock(QueryStore.class);
        Mockito.when(queryStore.getFieldMappings(Mockito.any()))
                .thenReturn(Mockito.mock(TableFieldMapping.class));
        queryExecutor = Mockito.mock(QueryExecutor.class);
        Mockito.when(queryExecutorFactory.getExecutor(Mockito.any()))
                .thenReturn(queryExecutor);
        Mockito.when(queryExecutor.execute(Mockito.any()))
                .thenReturn(Mockito.mock(ActionResponse.class));

        ObjectMapper objectMapper = new ObjectMapper();

        SerDe.init(objectMapper);
        this.fqlEngine = new FqlEngine(tableMetadataManager, queryStore, queryExecutorFactory, objectMapper);

    }

    @Test
    public void shouldPopulateRelevantSourceTypeForFqlQuery() throws JsonProcessingException {
        String query = "select * from payment where eventData.state = 'PAYMENT_COMPLETED' group by eventType";

        Map<String, String> requestTags = ImmutableMap.of(SourceType.SERVICE_NAME, "anomaly-detection");
        FqlRequest fqlRequest = new FqlRequest(query, false);
        fqlEngine.parse(fqlRequest, SourceType.SERVICE, requestTags);
        final ArgumentCaptor<ActionRequest> argumentCaptor = ArgumentCaptor.forClass(ActionRequest.class);
        Mockito.verify(queryExecutor, Mockito.times(1))
                .execute(argumentCaptor.capture());

        ActionRequest actionRequest = argumentCaptor.getValue();

        Assert.assertEquals(SourceType.SERVICE, actionRequest.getSourceType());
        Assert.assertEquals(requestTags, actionRequest.getRequestTags());
    }

    @Test
    public void shouldPopulateRelevantSourceTypeWithNullRequestTagsForFqlQuery() throws JsonProcessingException {
        String query = "select * from payment where eventData.state = 'PAYMENT_COMPLETED' group by eventType";
        FqlRequest fqlRequest = new FqlRequest(query, false);
        fqlEngine.parse(fqlRequest, SourceType.FQL, null);
        final ArgumentCaptor<ActionRequest> argumentCaptor = ArgumentCaptor.forClass(ActionRequest.class);
        Mockito.verify(queryExecutor, Mockito.times(1))
                .execute(argumentCaptor.capture());

        ActionRequest actionRequest = argumentCaptor.getValue();

        Assert.assertEquals(SourceType.FQL, actionRequest.getSourceType());
        Assert.assertNull(actionRequest.getRequestTags());
    }

    @Test
    public void shouldPopulateExtrapolationFlagFqlQuery() throws JsonProcessingException {
        String query = "select * from consumer_app where eventData.funnelInfo.funnelId = 7";

        Map<String, String> requestTags = ImmutableMap.of(SourceType.SERVICE_NAME, "anomaly-detection");
        FqlRequest fqlRequest = new FqlRequest(query, true);
        fqlEngine.parse(fqlRequest, SourceType.SERVICE, requestTags);
        final ArgumentCaptor<ActionRequest> argumentCaptor = ArgumentCaptor.forClass(ActionRequest.class);
        Mockito.verify(queryExecutor, Mockito.times(1))
                .execute(argumentCaptor.capture());

        ActionRequest actionRequest = argumentCaptor.getValue();

        Assert.assertEquals(true, actionRequest.isExtrapolationFlag());
        Assert.assertEquals(SourceType.SERVICE, actionRequest.getSourceType());
        Assert.assertEquals(requestTags, actionRequest.getRequestTags());
    }

    @Test
    public void testShowTableFqlQuery() throws JsonProcessingException {
        String query = "show tables";

        Map<String, String> requestTags = ImmutableMap.of(SourceType.SERVICE_NAME, "anomaly-detection");
        FqlRequest fqlRequest = new FqlRequest(query, true);
        fqlEngine.parse(fqlRequest, SourceType.SERVICE, requestTags);
        final ArgumentCaptor<ActionRequest> argumentCaptor = ArgumentCaptor.forClass(ActionRequest.class);
        Mockito.verify(tableMetadataManager, Mockito.times(1))
                .get();
    }

    @Test
    public void testDescribeTableFqlQuery() throws JsonProcessingException {
        String query = "desc payment";

        Map<String, String> requestTags = ImmutableMap.of(SourceType.SERVICE_NAME, "anomaly-detection");
        FqlRequest fqlRequest = new FqlRequest(query, true);
        fqlEngine.parse(fqlRequest, SourceType.SERVICE, requestTags);
        final ArgumentCaptor<ActionRequest> argumentCaptor = ArgumentCaptor.forClass(ActionRequest.class);
        Mockito.verify(queryStore, Mockito.times(1))
                .getFieldMappings(Mockito.any());
    }

}
