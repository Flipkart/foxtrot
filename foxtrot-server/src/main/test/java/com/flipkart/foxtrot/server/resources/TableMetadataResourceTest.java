package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableMetadataResourceTest extends ResourceTest {

    private TableMetadataManager tableMetadataManager;

    public TableMetadataResourceTest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ElasticsearchUtils.setMapper(mapper);

        //Initializing Cache Factory
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        // Ensure that table exists before saving/reading data from it
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        tableMetadataManager.start();
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableMetadataResource(tableMetadataManager));
    }


    @Test
    public void testSave() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 30);
        client().resource("/foxtrot/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);

        Table response = tableMetadataManager.get(table.getName());
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveNullTable() throws Exception {
        Table table = null;
        client().resource("/foxtrot/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveNullTableName() throws Exception {
        Table table = new Table(null, 30);
        client().resource("/foxtrot/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }

    @Test(expected = InvalidEntityException.class)
    public void testSaveIllegalTtl() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 0);
        client().resource("/foxtrot/v1/tables").type(MediaType.APPLICATION_JSON_TYPE).post(table);
    }


    @Test
    public void testGet() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 30);
        tableMetadataManager.save(table);

        Table response = client().resource(String.format("/foxtrot/v1/tables/%s", table.getName())).get(Table.class);
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        try {
            client().resource(String.format("/foxtrot/v1/tables/%s", TestUtils.TEST_TABLE)).get(Table.class);
        } catch (UniformInterfaceException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }
}
