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
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.*;
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
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableMetadataResourceTest extends ResourceTest {

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;

    public TableMetadataResourceTest() throws Exception {
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

        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        tableMetadataManager = spy(tableMetadataManager);
        tableMetadataManager.start();
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableMetadataResource(tableMetadataManager));
    }


    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }


    @Test
    public void testSave() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 30);
        client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);

        Table response = tableMetadataManager.get(table.getName());
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
        doThrow(new Exception()).when(tableMetadataManager).save(Matchers.<Table>any());
        try {
            client().resource("/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
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
        tableMetadataManager.save(table);

        Table response = client().resource(String.format("/v1/tables/%s", table.getName())).get(Table.class);
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        try {
            client().resource(String.format("/v1/tables/%s", TestUtils.TEST_TABLE_NAME)).get(Table.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }
}
