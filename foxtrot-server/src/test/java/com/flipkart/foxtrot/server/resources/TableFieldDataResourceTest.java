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
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldData;
import com.flipkart.foxtrot.common.TableFieldMetadata;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldDataResourceTest extends ResourceTest {

    private ObjectMapper mapper = new ObjectMapper();
    private HazelcastInstance hazelcastInstance;
    private MockElasticsearchServer elasticsearchServer;
    private QueryStore queryStore;

    public TableFieldDataResourceTest() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        tableMetadataManager.start();
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        QueryExecutor queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableFieldMetadataResource(queryStore));
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testGet() throws Exception {
        queryStore.save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments());
        Thread.sleep(500);

        Set<FieldData> mappings = new HashSet<FieldData>();
        mappings.add(new FieldData("word", FieldType.STRING));
        mappings.add(new FieldData("data.data", FieldType.STRING));
        mappings.add(new FieldData("header.hello", FieldType.STRING));
        mappings.add(new FieldData("head.hello", FieldType.LONG));

        TableFieldMetadata tableFieldMetadata = new TableFieldMetadata(TestUtils.TEST_TABLE_NAME, mappings);
        String response = client().resource(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .get(String.class);

        TableFieldMetadata mapping = mapper.readValue(response, TableFieldMetadata.class);
        assertEquals(tableFieldMetadata.getTable(), mapping.getTable());
        assertTrue(tableFieldMetadata.getFieldData().equals(mapping.getFieldData()));
    }

    @Test(expected = UniformInterfaceException.class)
    public void testGetInvalidTable() throws Exception {
        client().resource(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME + "-missing"))
                .get(String.class);
    }

    @Test
    public void testGetTableWithNoDocument() throws Exception {
        TableFieldMetadata request = new TableFieldMetadata(TestUtils.TEST_TABLE_NAME, new HashSet<FieldData>());
        TableFieldMetadata response = client().resource(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .get(TableFieldMetadata.class);

        assertEquals(request.getTable(), response.getTable());
        assertTrue(request.getFieldData().equals(response.getFieldData()));
    }
}
