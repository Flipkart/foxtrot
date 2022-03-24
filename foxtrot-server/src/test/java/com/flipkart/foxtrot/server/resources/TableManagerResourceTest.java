/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.FieldDataType;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.httpclient.HttpStatus;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

import static com.flipkart.foxtrot.common.FieldDataType.*;
import static com.flipkart.foxtrot.common.Table.DEFAULT_COLUMNS;
import static com.flipkart.foxtrot.core.TestUtils.TEST_TABLE_NAME;
import static com.flipkart.foxtrot.core.TestUtils.TEST_TENANT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableManagerResourceTest extends FoxtrotResourceTest {

    public static final String NON_EXISTENT_PIPELINE = "NON_EXISTENT_PIPELINE";
    @Rule
    public ResourceTestRule resources;

    private TableManager tableManager;
    private ElasticsearchTemplateMappingParser templateMappingParser;

    public TableManagerResourceTest() throws Exception {
        super();
        DataStore dataStore = mock(DataStore.class);
        Mockito.doNothing()
                .when(dataStore)
                .initializeTable(Mockito.any(Table.class), Mockito.anyBoolean());
        this.tableManager = new FoxtrotTableManager(getTableMetadataManager(), getQueryStore(), dataStore,
                getTenantMetadataManager(), getPipelineMetadataManager());
        this.templateMappingParser = getTemplateMappingParser();
        this.tableManager = spy(tableManager);
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new TableManagerResource(tableManager, getQueryStore()))
                .build();
    }


    @Test
    public void testSave() throws Exception {

        SortedMap<String, FieldDataType> customFieldMappings = new TreeMap<>();
        customFieldMappings.put("eventData.fees", FLOAT);
        customFieldMappings.put("eventData.cardTransaction", BOOLEAN);
        customFieldMappings.put("eventData.amount", INTEGER);
        customFieldMappings.put("eventData.context.ruleId", KEYWORD);
        customFieldMappings.put("eventData.context", NESTED);
        customFieldMappings.put("date", DATE);
        customFieldMappings.put("eventData.charge", DOUBLE);
        customFieldMappings.put("eventData.paymentInitRequest.to.amount", LONG);
        customFieldMappings.put("eventData.paymentInitRequest.to.vpa", TEXT);
        customFieldMappings.put("eventData.backEndErrorCode", TEXT);

        String tableName = "dummy_table";
        Table table = Table.builder()
                .name(tableName)
                .ttl(7)
                .tenantName(TEST_TENANT_NAME)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .customFieldMappings(customFieldMappings)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.target("/v1/tables")
                .request()
                .post(tableEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());

        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(
                ElasticsearchUtils.getMappingTemplateName(table.getName()));

        GetIndexTemplatesResponse indexTemplateResponse = elasticsearchConnection.getClient()
                .indices()
                .getIndexTemplate(getIndexTemplatesRequest, RequestOptions.DEFAULT);

        Assert.assertEquals(1, indexTemplateResponse.getIndexTemplates()
                .size());
        IndexTemplateMetaData indexTemplateMetaData = indexTemplateResponse.getIndexTemplates()
                .get(0);
        MappingMetaData mappings = indexTemplateMetaData.mappings();

        Map<String, FieldDataType> fieldMappings = templateMappingParser.getFieldMappings(mappings);
        Assert.assertEquals(FLOAT, fieldMappings.get("eventData.fees"));
        Assert.assertEquals(BOOLEAN, fieldMappings.get("eventData.cardTransaction"));
        Assert.assertEquals(INTEGER, fieldMappings.get("eventData.amount"));
        Assert.assertEquals(KEYWORD, fieldMappings.get("eventData.context.ruleId"));
        Assert.assertEquals(NESTED, fieldMappings.get("eventData.context"));
        Assert.assertEquals(DATE, fieldMappings.get("date"));
        Assert.assertEquals(DOUBLE, fieldMappings.get("eventData.charge"));
        Assert.assertEquals(LONG, fieldMappings.get("eventData.paymentInitRequest.to.amount"));
        Assert.assertEquals(TEXT, fieldMappings.get("eventData.paymentInitRequest.to.vpa"));
        Assert.assertEquals(TEXT, fieldMappings.get("eventData.backEndErrorCode"));
    }

    @Test
    public void testSaveNullTable() throws Exception {
        reset(tableManager);
        Response response = resources.target("/v1/tables")
                .request()
                .post(null);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveNullTableName() throws Exception {
        reset(tableManager);
        Table table = Table.builder()
                .name(null)
                .ttl(30)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.target("/v1/tables")
                .request()
                .post(tableEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveNonExistentPipeline() throws Exception {
        reset(tableManager);
        Table table = Table.builder()
                .name("dummy_table")
                .ttl(30)
                .shards(1)
                .defaultPipeline(NON_EXISTENT_PIPELINE)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        doThrow(FoxtrotExceptions.createPipelineMissingException("dummy")).when(tableManager)
                .save(Mockito.any(Table.class), Mockito.anyBoolean());
        Response response = resources.target("/v1/tables")
                .request()
                .post(tableEntity);
        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    }

    @Test
    public void testSaveBackendError() throws Exception {
        reset(tableManager);
        Table table = Table.builder()
                .name(UUID.randomUUID()
                        .toString())
                .ttl(30)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(tableManager)
                .save(Mockito.any(Table.class), Mockito.anyBoolean());
        Response response = resources.target("/v1/tables")
                .request()
                .post(tableEntity);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        reset(tableManager);
    }

    @Test
    public void testSaveIllegalTtl() throws Exception {
        reset(tableManager);
        Table table = Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(0)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.target("/v1/tables")
                .request()
                .post(tableEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    private void compare(Table expected,
                         Table actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual Table name should not be null", actual.getName());
        assertNotNull("Actual Tenant name should not be null", actual.getTenantName());
        assertEquals("Actual ttl should match expected ttl", actual.getTtl(), expected.getTtl());
    }

    @Test
    public void testGetTable() throws Exception {
        Table table = Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(7)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Table response = resources.target(String.format("/v1/tables/%s", TEST_TABLE_NAME))
                .request()
                .get(Table.class);
        compare(table, response);
    }

    @Test
    public void testGetAllTables() throws Exception {
        List<Table> tables = new ArrayList<>();
        Table table1 = Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(7)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        tables.add(table1);
        doReturn(tables).when(tableManager)
                .getAll();
        List<Table> response = resources.target("/v1/tables")
                .request()
                .get(Response.class)
                .readEntity(new GenericType<List<Table>>() {
                });
        assertNotNull(response);
        assertEquals(tables.size(), response.size());
        int i = 0;
        while (i < tables.size()) {
            compare(tables.get(i), response.get(i));
            i++;
        }
    }

    @Test
    public void testUpdateTable() throws Exception {
        Table table = Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(7)
                .shards(1)
                .columns(DEFAULT_COLUMNS)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build();
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.target(String.format("/v1/tables/%s", TEST_TABLE_NAME))
                .request()
                .put(tableEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        Response response = resources.target(String.format("/v1/tables/%s", TEST_TABLE_NAME + "_missing"))
                .request()
                .get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }
}
