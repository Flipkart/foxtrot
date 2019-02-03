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
package com.flipkart.foxtrot.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTime;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class.getSimpleName());
    public static final String TEST_TABLE_NAME = "test-table";
    public static final Table TEST_TABLE
            = Table.builder()
            .name(TEST_TABLE_NAME)
            .ttl(7)
            .build();

    public static DataStore getDataStore() throws FoxtrotException {
        org.apache.hadoop.hbase.client.Table tableInterface = MockHTable.create();
        HbaseTableConnection tableConnection = Mockito.mock(HbaseTableConnection.class);
        doReturn(tableInterface).when(tableConnection).getTable(Matchers.<Table>any());
        doReturn(new HbaseConfig()).when(tableConnection).getHbaseConfig();
        HBaseDataStore hBaseDataStore = new HBaseDataStore(tableConnection, new ObjectMapper(),
                new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV2()));
        hBaseDataStore = spy(hBaseDataStore);
        return hBaseDataStore;
    }

    public static Document getDocument(String id, long timestamp, Object[] args, ObjectMapper mapper) {
        Map<String, Object> data = Maps.newHashMap();
        for (int i = 0; i < args.length; i += 2) {
            data.put((String) args[i], args[i + 1]);
        }
        return new Document(id, timestamp, mapper.valueToTree(data));
    }

    public static void registerActions(AnalyticsLoader analyticsLoader, ObjectMapper mapper) throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actions = reflections.getSubTypesOf(Action.class);
        if (actions.isEmpty()) {
            throw new Exception("No analytics actions found!!");
        }
        List<NamedType> types = new Vector<>();
        for (Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if (null == analyticsProvider.request()
                    || null == analyticsProvider.opcode()
                    || analyticsProvider.opcode().isEmpty()
                    || null == analyticsProvider.response()) {
                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            }
            if (analyticsProvider.opcode().equalsIgnoreCase("default")) {
                logger.warn("Action " + action.getCanonicalName() + " does not specify cache token. " +
                        "Using default cache.");
            }
            analyticsLoader.register(new ActionMetadata(
                    analyticsProvider.request(), action,
                    analyticsProvider.cacheable(), analyticsProvider.opcode()));
            types.add(new NamedType(analyticsProvider.request(), analyticsProvider.opcode()));
            types.add(new NamedType(analyticsProvider.response(), analyticsProvider.opcode()));
            logger.info("Registered action: " + action.getCanonicalName());
        }
        mapper.getSubtypeResolver().registerSubtypes(types.toArray(new NamedType[types.size()]));
    }


    public static HbaseConfig createHBaseConfigWithRawKeyV1() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setRawKeyVersion("1.0");
        return hbaseConfig;
    }

    public static HbaseConfig createHBaseConfigWithRawKeyV2() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setRawKeyVersion("2.0");
        return hbaseConfig;
    }

    public static Document translatedDocumentWithRowKeyVersion1(Table table, Document document) {
        return new DocumentTranslator(createHBaseConfigWithRawKeyV1()).translate(table, document);
    }

    public static Document translatedDocumentWithRowKeyVersion2(Table table, Document document) {
        return new DocumentTranslator(createHBaseConfigWithRawKeyV2()).translate(table, document);
    }

    public static List<Document> getQueryDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117004L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117003L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117002L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117001L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        return documents;
    }

    public static List<Document> getGroupDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1397658118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1397658118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }

    public static List<Document> getGroupDocumentsForEstimation(ObjectMapper mapper) {
        Random random = new Random();
        return IntStream.rangeClosed(0, 2000)
                .mapToObj(i -> Document.builder()
                        .id(UUID.randomUUID().toString())
                        .timestamp(i * 60000 + System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                        .data(mapper.valueToTree(
                                ImmutableMap.<String, Object>builder()
                                        .put("deviceId", UUID.randomUUID().toString())
                                        .put("os",
                                                new String[]{
                                                        "ios",
                                                        "android",
                                                        "android",
                                                        "android"
                                                }[random.nextInt(2)])
                                        .put("registered",
                                                new boolean[]{
                                                        true,
                                                        false,
                                                        false
                                                }[random.nextInt(3)])
                                        .put("value", random.nextInt(101))
                                        .build()))
                        .build())
                .collect(Collectors.toList());
    }

    public static List<Document> getHistogramDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397651117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397651117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658218001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1398658218002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397758218003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397958118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1398653118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1398653118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }

    public static List<Document> getTrendDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397651117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397651117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658218001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1398658218002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397758218003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397958118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1398653118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1398653118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }

    public static List<Document> getStatsDocuments(ObjectMapper mapper) {
        List<Document> documents = Lists.newArrayList();
        documents.add(TestUtils.getDocument("Z", 1467282856000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 10}, mapper));
        documents.add(TestUtils.getDocument("Y", 1467331200000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 20}, mapper));
        documents.add(TestUtils.getDocument("X", 1467417600000L, new Object[]{"os", "ios", "version", 3, "device", "galaxy", "battery", 30}, mapper));
        documents.add(TestUtils.getDocument("W", 1467504000000L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 40}, mapper));
        documents.add(TestUtils.getDocument("A", 1467590400000L, new Object[]{"os", "wp", "version", 3, "device", "nexus", "battery", 50}, mapper));
        return documents;
    }

    public static List<Document> getStatsTrendDocuments(ObjectMapper mapper) {
        List<Document> documents = Lists.newArrayList();
        documents.add(TestUtils.getDocument("Z", 1467282856000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1467331200000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1467417600000L, new Object[]{"os", "ios", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1467504000000L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1467590400000L, new Object[]{"os", "wp", "version", 3, "device", "nexus", "battery", 87}, mapper));
        return documents;
    }

    public static List<Document> getCountDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397651117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397651117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658218001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1398658218002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397758218003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397958118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1398653118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1398653118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }

    public static List<Document> getDistinctDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397651117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("D", 1397758218003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397958118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1398653118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1398653118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }

    public static List<Document> getMappingDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<Document>();
        Map<String, Object> document = Maps.newHashMap();
        document.put("word", "1234");
        document.put("data", Collections.singletonMap("data", "d"));
        document.put("header", Collections.singletonList(Collections.singletonMap("hello", "world")));
        documents.add(new Document("Z", System.currentTimeMillis(), mapper.valueToTree(document)));

        document = Maps.newHashMap();
        document.put("word", "1234");
        document.put("data", Collections.singletonMap("data", "d"));
        document.put("head", Collections.singletonList(Collections.singletonMap("hello", 23)));
        documents.add(new Document("Y", System.currentTimeMillis(), mapper.valueToTree(document)));
        return documents;
    }

    public static List<Document> getFieldCardinalityEstimationDocuments(ObjectMapper mapper) {
        List<Document> documents = new Vector<>();
        Map<String, Object> document = Maps.newHashMap();
        document.put("word", "1234");
        document.put("numeric", 20);
        document.put("boolean", true);
        document.put("data", Collections.singletonMap("someField", "d"));
        document.put("header", Collections.singletonList(Collections.singletonMap("someHeaderField", "client1")));
        final long time = DateTime.now().minusDays(1).toDate().getTime();
//        final long time = System.currentTimeMillis();
        documents.add(new Document("Z", time, mapper.valueToTree(document)));

        document = Maps.newHashMap();
        document.put("word", "2345");
        document.put("numeric", 30);
        document.put("boolean", true);
        document.put("data", ImmutableMap.of(
                "someField", "c", "someOtherField", "blah", "exclusiveField", "hmmm"));
        document.put("header", Collections.singletonList(Collections.singletonMap("someHeaderField", "client1")));
        documents.add(new Document("Y", time, mapper.valueToTree(document)));

        document = Maps.newHashMap();
        document.put("word", "2345");
        document.put("numeric", 25);
        document.put("boolean", false);
        document.put("data", ImmutableMap.of("someField", "c", "someOtherField", "blah"));
        document.put("header", Collections.singletonList(Collections.singletonMap("someHeaderField", "client1")));
        documents.add(new Document("X", time, mapper.valueToTree(document)));

        return documents;
    }

    public static List<Document> getQueryDocumentsDifferentDate(ObjectMapper mapper, long startTimestamp) {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", startTimestamp, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", startTimestamp++, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", startTimestamp++, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", startTimestamp++, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", startTimestamp++, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", startTimestamp++, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("C", startTimestamp++, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("D", startTimestamp++, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("E", startTimestamp, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        return documents;
    }

    public static ElasticsearchConnection initESConnection(MockElasticsearchServer elasticsearchServer) {
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX);
        Settings indexSettings = Settings.builder().put("number_of_replicas", 0).build();
        createRequest.settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());
        return elasticsearchConnection;
    }
}
