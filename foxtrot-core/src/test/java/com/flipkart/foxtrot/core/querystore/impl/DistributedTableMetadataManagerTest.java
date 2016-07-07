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
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class DistributedTableMetadataManagerTest {
    private HazelcastInstance hazelcastInstance;
    private DistributedTableMetadataManager distributedTableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private IMap<String, Table> tableDataStore;

    @Before
    public void setUp() throws Exception {
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(GroupResponse.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        Config config = new Config();
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);

        //Create index for table meta. Not created automatically
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX);
        Settings indexSettings = Settings.builder().put("number_of_replicas", 0).build();
        createRequest.settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        String DATA_MAP = "tablemetadatamap";
        tableDataStore = hazelcastInstance.getMap(DATA_MAP);
        distributedTableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        distributedTableMetadataManager.start();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
        elasticsearchServer.shutdown();
        distributedTableMetadataManager.stop();
    }

    @Test
    public void testSave() throws Exception {
        Table table = new Table();
        table.setName("TEST_TABLE");
        table.setTtl(15);
        distributedTableMetadataManager.save(table);
        Table responseTable = distributedTableMetadataManager.get("TEST_TABLE");
        assertEquals(table.getName(), responseTable.getName());
        assertEquals(table.getTtl(), responseTable.getTtl());
    }

    @Test
    public void testGet() throws Exception {
        Table table = new Table();
        table.setName(TestUtils.TEST_TABLE_NAME);
        table.setTtl(60);
        tableDataStore.put(TestUtils.TEST_TABLE_NAME, table);
        Table response = distributedTableMetadataManager.get(table.getName());
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test
    public void testGetMissingTable() throws Exception {
        Table response = distributedTableMetadataManager.get(TestUtils.TEST_TABLE + "-missing");
        assertNull(response);
    }

    @Test
    public void testExists() throws Exception {
        Table table = new Table();
        table.setName(TestUtils.TEST_TABLE_NAME);
        table.setTtl(15);
        distributedTableMetadataManager.save(table);
        assertTrue(distributedTableMetadataManager.exists(table.getName()));
        assertFalse(distributedTableMetadataManager.exists("DUMMY_TEST_NAME_NON_EXISTENT"));
    }
}
