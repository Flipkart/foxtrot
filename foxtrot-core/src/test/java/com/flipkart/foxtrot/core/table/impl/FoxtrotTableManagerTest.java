package com.flipkart.foxtrot.core.table.impl;

import com.flipkart.foxtrot.common.FieldDataType;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.ActionTest;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.flipkart.foxtrot.common.FieldDataType.*;
import static com.flipkart.foxtrot.common.Table.DEFAULT_COLUMNS;
import static com.flipkart.foxtrot.core.TestUtils.TEST_TENANT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
public class FoxtrotTableManagerTest extends ActionTest {

    private TableManager tableManager;
    private QueryStore queryStore;
    private DataStore dataStore;
    private TableMetadataManager tableMetadataManager;
    private TenantMetadataManager tenantMetadataManager;
    private PipelineMetadataManager pipelineMetadataManager;

    @Before
    public void setUp() throws Exception {
        this.queryStore = getQueryStore();
        this.dataStore = mock(DataStore.class);
        this.tableMetadataManager = mock(TableMetadataManager.class);
        this.tenantMetadataManager = mock(TenantMetadataManager.class);
        this.pipelineMetadataManager = mock(PipelineMetadataManager.class);
        this.tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore, tenantMetadataManager,
                pipelineMetadataManager);
    }

    @Test
    public void testSaveTableNullName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName(null);
            table.setTtl(60);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveTableEmptyName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName(" ");
            table.setTtl(60);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveNullTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            tableManager.save(null);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveTableInvalidTtl() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(0);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveTableNullTenantName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(0);
            table.setTenantName(null);
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveTableEmptyTenantName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(0);
            table.setTenantName(" ");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testSaveTableInvalidTenantName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tenantMetadataManager)
                .exists(any(String.class));
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setTenantName("tenant");
            tableManager.save(table);
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TENANT_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testSaveTableInvalidPipelineName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(true).when(tenantMetadataManager)
                .exists(any(String.class));
        doReturn(false).when(pipelineMetadataManager)
                .exists(any(String.class));
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setDefaultPipeline("pipeline");
            table.setTenantName("tenant");
            tableManager.save(table);
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.PIPELINE_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testSaveTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(true).when(tenantMetadataManager)
                .exists(any(String.class));

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

        tableManager.save(table);

        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(
                ElasticsearchUtils.getMappingTemplateName(table.getName()));

        GetIndexTemplatesResponse indexTemplateResponse = getElasticsearchConnection().getClient()
                .indices()
                .getIndexTemplate(getIndexTemplatesRequest, RequestOptions.DEFAULT);

        Assert.assertEquals(1, indexTemplateResponse.getIndexTemplates()
                .size());
        IndexTemplateMetaData indexTemplateMetaData = indexTemplateResponse.getIndexTemplates()
                .get(0);
        MappingMetaData mappings = indexTemplateMetaData.mappings();

        Map<String, FieldDataType> fieldMappings = getTemplateMappingParser().getFieldMappings(mappings);
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
    public void testSaveExistingTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(true).when(tableMetadataManager)
                .exists(any(String.class));
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TABLE_ALREADY_EXISTS, e.getCode());
        }
    }

    @Test
    public void testSaveTableQueryStoreFailed() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doReturn(true).when(tenantMetadataManager)
                .exists(any(String.class));
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setShards(1);
            table.setColumns(5000);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.STORE_EXECUTION_ERROR, e.getCode());
        }
    }

    @Test
    public void testSaveTableDataStoreFailed() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doReturn(true).when(tenantMetadataManager)
                .exists(any(String.class));
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setShards(1);
            table.setColumns(5000);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.STORE_EXECUTION_ERROR, e.getCode());
        }
    }

    @Test
    public void testSaveTableDataStoreNoTableFound() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doReturn(true).when(tenantMetadataManager)
                .exists(any(String.class));
        doThrow(FoxtrotExceptions.createTableMissingException("Dummy")).when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setShards(1);
            table.setColumns(5000);
            table.setTenantName("tenant");
            tableManager.save(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testUpdateTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(true).when(tableMetadataManager)
                .exists(anyString());
        doReturn(true).when(tenantMetadataManager)
                .exists(anyString());
        Table table = new Table();
        table.setName("abcd");
        table.setShards(1);
        table.setColumns(5000);
        table.setTtl(10);
        table.setTenantName("tenant");
        tableManager.update(table);
        Assert.assertTrue(true);
    }

    @Test
    public void testUpdateNonExistingTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tableMetadataManager)
                .exists(anyString());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(10);
            table.setTenantName("tenant");
            tableManager.update(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testUpdateNullTable() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tableMetadataManager)
                .exists(anyString());
        try {
            tableManager.update(null);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testUpdateTableNullName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tableMetadataManager)
                .exists(anyString());
        try {
            Table table = new Table();
            table.setName(null);
            table.setTtl(10);
            table.setTenantName("tenant");
            tableManager.update(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testUpdateTableEmptyName() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tableMetadataManager)
                .exists(anyString());
        try {
            Table table = new Table();
            table.setName(" ");
            table.setTtl(10);
            table.setTenantName("tenant");
            tableManager.update(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testUpdateTableInvalidTtl() throws Exception {
        doNothing().when(tableMetadataManager)
                .save(any(Table.class));
        doNothing().when(dataStore)
                .initializeTable(any(Table.class), anyBoolean());
        doReturn(false).when(tableMetadataManager)
                .exists(anyString());
        try {
            Table table = new Table();
            table.setName("abcd");
            table.setTtl(0);
            table.setTenantName("tenant");
            tableManager.update(table);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.INVALID_REQUEST, e.getCode());
        }
    }

    @Test
    public void testGetTableSuccess() throws Exception {
        Table table = new Table();
        table.setName("table");
        table.setTtl(10);
        table.setTenantName("tenant");

        doReturn(table).when(tableMetadataManager)
                .get("table");
        Table getTable = tableManager.get("table");
        assertEquals("table", getTable.getName());
        assertEquals(10, getTable.getTtl());
        assertEquals("tenant", getTable.getTenantName());
    }

    @Test
    public void testGetFailureForNullTable() throws Exception {
        doReturn(null).when(tableMetadataManager)
                .get("table");
        try {
            Table getTable = tableManager.get("table");
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testGetAllSuccess() throws Exception {
        Table table = new Table();
        table.setName("table");
        table.setTtl(10);
        table.setTenantName("tenant");

        doReturn(Arrays.asList(table)).when(tableMetadataManager)
                .get();
        List<Table> getAllTable = tableManager.getAll();
        assertEquals(1, getAllTable.size());
        assertEquals("table", getAllTable.get(0)
                .getName());
        assertEquals(10, getAllTable.get(0)
                .getTtl());
        assertEquals("tenant", getAllTable.get(0)
                .getTenantName());
    }

    @Test
    public void getAllEmpty() throws Exception {
        doReturn(Collections.emptyList()).when(tableMetadataManager)
                .get();

        List<Table> getAllTable = tableManager.getAll();
        assertEquals(0, getAllTable.size());
    }
}