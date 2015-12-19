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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.StoreExecutionException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableManagerResourceTest extends ResourceTest {

    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private TableManager tableManager;

    public TableManagerResourceTest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ElasticsearchUtils.setMapper(mapper);

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));
        Config config = new Config();
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        tableMetadataManager = spy(tableMetadataManager);
        tableMetadataManager.start();

        QueryStore queryStore = Mockito.mock(QueryStore.class);
        DataStore dataStore = Mockito.mock(DataStore.class);
        this.tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        this.tableManager = spy(tableManager);
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableManagerResource(tableManager));
        addProvider(FoxtrotExceptionMapper.class);
    }


    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }


    @Test
    public void testSave() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);

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
        doThrow(new StoreExecutionException("dummy", new IOException())).when(tableManager).save(Matchers.<Table>any());
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
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        tableManager.save(table);

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
