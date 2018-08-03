/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class DistributedTableMetadataManagerTest {
    private DataStore dataStore;
    private ElasticsearchQueryStore queryStore;
    private HazelcastInstance hazelcastInstance;
    private DistributedTableMetadataManager distributedTableMetadataManager;
    private IMap<String, Table> tableDataStore;
    private ElasticsearchConnection elasticsearchConnection;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws Exception {
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        objectMapper = new ObjectMapper();
        objectMapper.registerSubtypes(GroupResponse.class);

        this.dataStore = Mockito.mock(DataStore.class);

        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());

        /*ElasticsearchContainer container = new ElasticsearchContainer();
        container.withVersion("6.0.1");
        container.withBaseUrl("docker.elastic.co/elasticsearch/elasticsearch");
        //container.withPlugin("discovery-gce");
        //container.withPluginDir(Paths.get("/path/to/zipped-plugins-dir"));
        container.withEnv("ELASTIC_PASSWORD", "foxtrot");
        container.start();

        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setCluster("test");
        config.setHosts("localhost");
        config.setTableNamePrefix("foxtrot");
        config.setPort(container.getHost().getPort());

        elasticsearchConnection = new ElasticsearchConnection(config);
        elasticsearchConnection.start();
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());*/


        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        hazelcastConnection.start();

        distributedTableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        distributedTableMetadataManager.start();

        tableDataStore = hazelcastInstance.getMap("tablemetadatamap");
        this.queryStore = new ElasticsearchQueryStore(distributedTableMetadataManager, elasticsearchConnection,
                dataStore, objectMapper);
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
            elasticsearchConnection.getClient().admin().indices().delete(deleteIndexRequest);
        } catch (Exception e) {
            //Do Nothing
        }
        elasticsearchConnection.stop();
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
