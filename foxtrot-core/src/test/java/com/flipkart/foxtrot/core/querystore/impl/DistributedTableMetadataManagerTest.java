package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class DistributedTableMetadataManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(DistributedTableMetadataManagerTest.class.getSimpleName());
    private HazelcastInstance hazelcastInstance;
    private ObjectMapper mapper;
    private JsonNodeFactory factory;
    private DistributedTableMetadataManager distributedTableMetadataManager;
    private MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer();
    private IMap<String, Table> tableDataStore;
    private final String DATA_MAP = "tablemetadatamap";
    private final String TEST_APP = "test-app";

    @Before
    public void setUp() throws Exception {
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        mapper = new ObjectMapper();
        mapper.registerSubtypes(GroupResponse.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        factory = JsonNodeFactory.instance;

        //Create index for table meta. Not created automatically
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX);
        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        createRequest.settings(indexSettings);
        elasticsearchServer.getClient().admin().indices()
                .create(createRequest)
                .actionGet();
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

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
        logger.info("Testing - Distributed Table Metadata Manager - Save");
        Table table = new Table();
        table.setName("TEST_TABLE");
        table.setTtl(15);
        distributedTableMetadataManager.save(table);
        Table responseTable = (Table) hazelcastInstance.getMap(DistributedTableMetadataManager.DATA_MAP).get(table.getName());
        assertEquals(table.getName(), responseTable.getName());
        assertEquals(table.getTtl(), responseTable.getTtl());
        logger.info("Tested - Distributed Table Metadata Manager - Save");
    }

    @Test
    public void testGet() throws Exception {
        logger.info("Testing - Distributed Table Metadata Manager - GET");
        Table table = new Table();
        table.setName(TEST_APP);
        table.setTtl(60);
        tableDataStore.put(TEST_APP, table);
        Table response = distributedTableMetadataManager.get(table.getName());
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
        logger.info("Tested - Distributed Table Metadata Manager - GET");
    }

    @Test
    public void testGetMissingTable() throws Exception {
        logger.info("Tested - Distributed Table Metadata Manager - GET - Missing Table");
        Table response = distributedTableMetadataManager.get(TEST_APP + "-missing");
        assertNull(response);
        logger.info("Tested - Distributed Table Metadata Manager - GET - Missing Table");
    }

    @Test
    public void testExists() throws Exception {
        logger.info("Testing - Distributed Table Metadata Manager - Exists");
        Table table = new Table();
        table.setName("TEST_TABLE");
        table.setTtl(15);
        distributedTableMetadataManager.save(table);
        assertTrue(distributedTableMetadataManager.exists(table.getName()));
        assertFalse(distributedTableMetadataManager.exists("DUMMY_TEST_NAME_NON_EXISTENT"));
        logger.info("Tested - Distributed Table Metadata Manager - Exists");
    }
}
