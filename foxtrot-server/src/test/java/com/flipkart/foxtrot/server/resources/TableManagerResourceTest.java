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
import com.flipkart.foxtrot.core.table.TableManager;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
* Created by rishabh.goyal on 04/05/14.
*/
public class TableManagerResourceTest {
    private static TableManager tableManager = mock(TableManager.class);

    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder().addResource(new TableManagerResource(tableManager)).build();

    @Before
    public void setUp() throws Exception {
        reset(tableManager);
    }

    @Test
    public void testSave() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        doNothing().when(tableManager).save(any(Table.class));
        Response response = resource.client().target("/v1/tables").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(table));

        Table responseTable = response.readEntity(Table.class);

        assertNotNull(responseTable);
        assertEquals(table.getName(), responseTable.getName());
        assertEquals(table.getTtl(), responseTable.getTtl());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveNullTable() throws Exception {
        Table table = null;
        Response response = resource.client().target("/v1/tables").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(table));
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveNullTableName() throws Exception {
        Table table = new Table(null, 30);
        Response response = resource.client().target("/v1/tables").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(table));
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Table table = new Table(UUID.randomUUID().toString(), 30);
        doThrow(new Exception()).when(tableManager).save(any(Table.class));
        Response response = resource.client().target("/v1/tables").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(table));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testSaveIllegalTtl() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 0);
        Response response = resource.client().target("/v1/tables").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(table));
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testGet() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        doReturn(table).when(tableManager).get(anyString());

        Response response = resource.client().target(String.format("/v1/tables/%s", table.getName())).request().get();
        Table responseTable = response.readEntity(Table.class);

        assertNotNull(responseTable);
        assertEquals(table.getName(), responseTable.getName());
        assertEquals(table.getTtl(), responseTable.getTtl());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetList() throws Exception {
        Table tableOne = new Table(TestUtils.TEST_TABLE_NAME, 30);
        Table tableTwo = new Table(TestUtils.TEST_TABLE_NAME, 30);
        List<Table> tables = Arrays.asList(tableOne, tableTwo);

        doReturn(tables).when(tableManager).getAll();

        Response response = resource.client().target("/v1/tables").request().get();
        List<Table> responseTables = response.readEntity(List.class);

        assertEquals(tables.size(), responseTables.size());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testDelete() throws Exception {
        doNothing().when(tableManager).delete(anyString());
        Response response = resource.client().target("/v1/tables/random/delete").request().delete();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void testDeleteError() throws Exception {
        doThrow(new Exception()).when(tableManager).delete(anyString());
        Response response = resource.client().target("/v1/tables/random/delete").request().delete();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        doReturn(null).when(tableManager).get(anyString());
        Response response = resource.client().target(String.format("/v1/tables/%s", TestUtils.TEST_TABLE_NAME)).request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }
}
