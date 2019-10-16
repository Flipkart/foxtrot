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
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
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
        objectMapper = new ObjectMapper();
        objectMapper.registerSubtypes(GroupResponse.class);

        this.dataStore = Mockito.mock(DataStore.class);

        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setHost("127.0.0.1");

        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        hazelcastConnection.start();

        this.distributedTableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                                                                                   elasticsearchConnection,
                                                                                   objectMapper,
                                                                                   new CardinalityConfig());
        distributedTableMetadataManager.start();

        tableDataStore = hazelcastInstance.getMap("tablemetadatamap");
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(objectMapper,
                                                                                         TextNodeRemoverConfiguration.builder()
                                                                                                 .build()));
        this.queryStore = new ElasticsearchQueryStore(distributedTableMetadataManager, elasticsearchConnection,
                                                      dataStore, mutators, objectMapper, new CardinalityConfig());
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
            elasticsearchConnection.getClient()
                    .admin()
                    .indices()
                    .delete(deleteIndexRequest);
        }
        catch (Exception e) {
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

    @Test
    public void testGetFieldMappings() throws Exception {
        Table table = new Table();
        table.setName(TestUtils.TEST_TABLE_NAME);
        table.setTtl(15);
        distributedTableMetadataManager.save(table);

        Document document = TestUtils.getDocument("A", new DateTime().minusDays(1)
                .getMillis(), new Object[]{"os", "android", "version", 1}, objectMapper);
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, document);
        doReturn(translatedDocument).when(dataStore)
                .save(table, document);
        queryStore.save(TestUtils.TEST_TABLE_NAME, document);

        document = TestUtils.getDocument("B", new DateTime().getMillis(),
                                         new Object[]{"os", "android", "version", "abcd"}, objectMapper);
        translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, document);
        doReturn(translatedDocument).when(dataStore)
                .save(table, document);
        queryStore.save(TestUtils.TEST_TABLE_NAME, document);

        TableFieldMapping tableFieldMapping = distributedTableMetadataManager.getFieldMappings(
                TestUtils.TEST_TABLE_NAME, true, true);
        assertEquals(11, tableFieldMapping.getMappings()
                .size());

        assertEquals(FieldType.STRING, tableFieldMapping.getMappings()
                .stream()
                .filter(x -> x.getField()
                        .equals("version"))
                .findAny()
                .get()
                .getType());
        assertEquals(FieldType.STRING, tableFieldMapping.getMappings()
                .stream()
                .filter(x -> x.getField()
                        .equals("os"))
                .findAny()
                .get()
                .getType());
    }
}
