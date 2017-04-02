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

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableManagerResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    private TableManager tableManager;

    public TableManagerResourceTest() throws Exception {
        super();
        this.tableManager = new FoxtrotTableManager(getTableMetadataManager(), getQueryStore(), getDataStore());
        this.tableManager = spy(tableManager);
        resources = ResourceTestRule.builder()
                .addResource(new TableManagerResource(tableManager))
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .setMapper(getMapper())
                .build();
    }


    @Test
    public void testSave() throws Exception {
        doNothing().when(getDataStore()).initializeTable(any(Table.class));
        doNothing().when(getQueryStore()).initializeTable(any(String.class));

        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        Entity<Table> tableEntity = Entity.json(table);
        resources.client().target("/v1/tables").request().post(tableEntity);

        doReturn(table).when(getTableMetadataManager()).get(anyString());
        Table response = tableManager.get(table.getName());
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
        reset(tableManager);
        reset(getQueryStore());
        reset(getDataStore());
    }

    @Test
    public void testSaveNullTable() throws Exception {
        Response response = resources.client().target("/v1/tables").request().post(null);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveNullTableName() throws Exception {
        Table table = new Table(null, 30);
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.client().target("/v1/tables").request().post(tableEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Table table = new Table(UUID.randomUUID().toString(), 30);
        Entity<Table> tableEntity = Entity.json(table);
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(tableManager).save(Matchers.<Table>any());
        Response response = resources.client().target("/v1/tables").request().post(tableEntity);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        reset(tableManager);
    }

    @Test
    public void testSaveIllegalTtl() throws Exception {
        reset(getTableMetadataManager());
        reset(tableManager);
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 0);
        Entity<Table> tableEntity = Entity.json(table);
        Response response = resources.client().target("/v1/tables").request().post(tableEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testGet() throws Exception {
        doNothing().when(getDataStore()).initializeTable(any(Table.class));
        doNothing().when(getQueryStore()).initializeTable(any(String.class));

        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        tableManager.save(table);
        doReturn(table).when(getTableMetadataManager()).get(anyString());

        Table response = resources.client().target(String.format("/v1/tables/%s", table.getName())).request().get(Table.class);
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        Response response = resources.client().target(String.format("/v1/tables/%s", TestUtils.TEST_TABLE_NAME + "_missing")).request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }
}
