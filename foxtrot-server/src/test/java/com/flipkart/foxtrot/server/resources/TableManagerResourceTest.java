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
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.junit.Test;
import org.mockito.Matchers;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableManagerResourceTest extends FoxtrotResourceTest {

    private TableManager tableManager;

    public TableManagerResourceTest() throws Exception {
        super();
        this.tableManager = new FoxtrotTableManager(getTableMetadataManager(), getQueryStore(), getDataStore());
        this.tableManager = spy(tableManager);
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableManagerResource(tableManager));
        addProvider(FoxtrotExceptionMapper.class);
    }


    @Test
    public void testSave() throws Exception {
        doNothing().when(getDataStore()).initializeTable(any(Table.class));
        doNothing().when(getQueryStore()).initializeTable(any(String.class));

        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);

        doReturn(table).when(getTableMetadataManager()).get(anyString());
        Table response = tableManager.get(table.getName());
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveNullTable() throws Exception {
        Table table = null;
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveNullTableName() throws Exception {
        Table table = new Table(null, 30);
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Table table = new Table(UUID.randomUUID().toString(), 30);
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(tableManager).save(Matchers.<Table>any());
        try {
            client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveIllegalTtl() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 0);
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }


    @Test
    public void testGet() throws Exception {
        doNothing().when(getDataStore()).initializeTable(any(Table.class));
        doNothing().when(getQueryStore()).initializeTable(any(String.class));

        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        tableManager.save(table);
        doReturn(table).when(getTableMetadataManager()).get(anyString());

        Table response = client().resource(String.format("/v1/tables/%s", table.getName())).get(Table.class);
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        try {
            client().resource(String.format("/v1/tables/%s", TestUtils.TEST_TABLE_NAME)).get(Table.class);
            fail();
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }
}
