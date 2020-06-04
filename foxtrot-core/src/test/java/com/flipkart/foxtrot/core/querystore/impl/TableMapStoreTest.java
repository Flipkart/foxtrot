/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Created by rishabh.goyal on 02/05/14.
 */
@Slf4j
public class TableMapStoreTest {

    public static final String TEST_TABLE = "test-table";
    public static final String TABLE_META_INDEX = "table-meta";
    public static final String TABLE_META_TYPE = "table-meta";
    private static ElasticsearchConnection elasticsearchConnection;
    private ObjectMapper mapper = new ObjectMapper();
    private TableMapStore tableMapStore;


    @BeforeClass
    public static void setupClass() throws Exception {
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        elasticsearchConnection.stop();
    }

    @Before
    public void setUp() throws Exception {
        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TableMapStore.Factory factory = new TableMapStore.Factory(elasticsearchConnection);
        tableMapStore = factory.newMapStore(null, null);
    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }


    @Test
    public void testStore() throws Exception {
        Table table = new Table();
        table.setName(TEST_TABLE);
        table.setTtl(30);
        tableMapStore.store(table.getName(), table);

        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, table.getName()), RequestOptions.DEFAULT);
        compareTables(table, mapper.readValue(response.getSourceAsBytes(), Table.class));
    }

    private void compareTables(Table expected,
                               Table actual) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getTtl(), actual.getTtl());
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullKey() throws Exception {
        Table table = new Table();
        table.setName(TEST_TABLE);
        table.setTtl(30);
        tableMapStore.store(null, table);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullTable() throws Exception {
        Table table = new Table();
        table.setName(TEST_TABLE);
        table.setTtl(30);
        tableMapStore.store(table.getName(), null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreNullTableName() throws Exception {
        Table table = new Table();
        table.setName(null);
        table.setTtl(30);
        tableMapStore.store(TEST_TABLE, table);
    }

    @Test
    public void testStoreAll() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
        }
        tableMapStore.storeAll(tables);
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        tables.keySet()
                .forEach(key -> multiGetRequest.add(TABLE_META_INDEX, TABLE_META_TYPE, key));
        MultiGetResponse response = elasticsearchConnection.getClient()
                .mget(multiGetRequest, RequestOptions.DEFAULT);
        Map<String, Table> responseTables = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            Table table = mapper.readValue(multiGetItemResponse.getResponse()
                    .getSourceAsString(), Table.class);
            responseTables.put(table.getName(), table);
        }
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            compareTables(entry.getValue(), responseTables.get(entry.getKey()));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNull() throws Exception {
        tableMapStore.storeAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTableKey() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(null, table);
        }
        tableMapStore.storeAll(tables);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTableValue() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            tables.put(UUID.randomUUID()
                    .toString(), null);
        }
        tableMapStore.storeAll(tables);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllNullTableKeyValue() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            tables.put(null, null);
        }
        tableMapStore.storeAll(tables);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeys() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
        }

        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());
        table.setTtl(20);
        tables.put(null, table);
        tableMapStore.storeAll(tables);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullValues() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
        }

        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());
        table.setTtl(20);
        tables.put(table.getName(), null);
        tableMapStore.storeAll(tables);
    }

    @Test(expected = RuntimeException.class)
    public void testStoreAllSomeNullKeyValues() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
        }
        tables.put(null, null);
        tableMapStore.storeAll(tables);
    }

    @Test
    public void testDelete() throws Exception {
        Table table = new Table();
        table.setName(TEST_TABLE);
        table.setTtl(30);
        tableMapStore.store(table.getName(), table);
        GetResponse response = elasticsearchConnection.getClient()
                .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, table.getName()), RequestOptions.DEFAULT);
        assertTrue(response.isExists());

        tableMapStore.delete(table.getName());
        response = elasticsearchConnection.getClient()
                .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, table.getName()), RequestOptions.DEFAULT);
        assertFalse(response.isExists());
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteNullKey() throws Exception {
        tableMapStore.delete(null);
    }

    //TODO Why it didn't throw any exception ?
    @Test
    public void testDeleteMissingKey() throws Exception {
        tableMapStore.delete("HELLO");
        assertTrue(true);
    }

    @Test
    public void testDeleteAll() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
        }
        tableMapStore.storeAll(tables);
        for (String name : tables.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertTrue(response.isExists());
        }

        tableMapStore.deleteAll(tables.keySet());
        for (String name : tables.keySet()) {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, name), RequestOptions.DEFAULT);
            ;
            assertFalse(response.isExists());
        }

    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNull() throws Exception {
        tableMapStore.deleteAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteAllNullKeys() throws Exception {
        List<String> keys = new ArrayList<String>();
        keys.add(null);
        keys.add(null);
        tableMapStore.deleteAll(keys);
    }

    @Test
    public void testLoad() throws Exception {
        Table table = new Table();
        table.setName(TEST_TABLE);
        table.setTtl(30);
        Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, table);
        elasticsearchConnection.getClient()
                .index(new IndexRequest(TABLE_META_INDEX).type(TABLE_META_TYPE)
                        .source(sourceMap)
                        .id(table.getName())
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        Table responseTable = tableMapStore.load(table.getName());
        compareTables(table, responseTable);
    }

    @Test
    public void testLoadMissingKey() throws Exception {
        assertNull(tableMapStore.load(UUID.randomUUID()
                .toString()));
    }

    //TODO Why not an error ?
    @Test
    public void testLoadNullKey() throws Exception {
        assertNull(tableMapStore.load(UUID.randomUUID()
                .toString()));
    }

    // Exception Caught because of Runtime. Not an IOException
    @Test(expected = RuntimeException.class)
    public void testLoadKeyWithWrongJson() throws Exception {
        elasticsearchConnection.getClient()
                .index(new IndexRequest(TABLE_META_INDEX).type(TABLE_META_TYPE)
                        .source("{ \"test\" : \"test\"}")
                        .id(TEST_TABLE)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        tableMapStore.load(TEST_TABLE);
    }

    @Test
    public void testLoadAll() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, table);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(TABLE_META_INDEX).type(TABLE_META_TYPE)
                            .source(sourceMap)
                            .id(table.getName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> names = ImmutableSet.copyOf(Iterables.limit(tables.keySet(), 5));
        Map<String, Table> responseTables = tableMapStore.loadAll(names);
        assertEquals(names.size(), responseTables.size());
        for (String name : names) {
            compareTables(tables.get(name), responseTables.get(name));
        }
    }

    @Test(expected = NullPointerException.class)
    public void testLoadAllNull() throws Exception {
        tableMapStore.loadAll(null);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadAllKeyWithWrongJson() throws Exception {
        elasticsearchConnection.getClient()
                .index(new IndexRequest(TABLE_META_INDEX).type(TABLE_META_TYPE)
                        .source("{ \"test\" : \"test\"}")
                        .id(TEST_TABLE)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        tableMapStore.loadAll(Arrays.asList(TEST_TABLE));
    }

    @Test
    public void testLoadAllKeys() throws Exception {
        Map<String, Table> tables = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Table table = new Table();
            table.setName(UUID.randomUUID()
                    .toString());
            table.setTtl(20);
            tables.put(table.getName(), table);
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(mapper, table);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(TABLE_META_INDEX).type(TABLE_META_TYPE)
                            .source(sourceMap)
                            .id(table.getName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }

        Set<String> responseKeys = tableMapStore.loadAllKeys();
        for (String name : tables.keySet()) {
            assertTrue(responseKeys.contains(name));
        }
    }
}
