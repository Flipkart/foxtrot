package com.flipkart.foxtrot.core.indexmeta.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.util.StorageSizeUtils;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.joda.time.DateTime;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

public class IndexMetadataMapStoreTest {

    private static HazelcastConnection hazelcastConnection;

    private static ElasticsearchConnection elasticsearchConnection;
    private static IndexMetadataManager indexMetadataManager;
    private static IndexMetadataMapStore indexMetadataMapStore;

    @BeforeClass
    public static void setup() throws Exception {
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());

        ObjectMapper mapper = new ObjectMapper();
        SerDe.init(mapper);
        hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(hazelcastInstance.getConfig());

        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        TestUtils.ensureIndex(elasticsearchConnection, IndexMetadataManagerImpl.INDEX_METADATA_INDEX);
        indexMetadataMapStore = new IndexMetadataMapStore(elasticsearchConnection);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastConnection.getHazelcast()
                .shutdown();
        elasticsearchConnection.stop();
    }

    @Before
    public void beforeMethod() throws IOException {
        TestUtils.ensureIndex(elasticsearchConnection, IndexMetadataManagerImpl.INDEX_METADATA_INDEX);
    }

    @After
    public void afterMethod() {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }

    @Test
    public void shouldSaveAllIndicesMetadata() {
        String tableIndex1 = "foxtrot-payment-table-04-10-2021";
        DateTime currentTime = new DateTime();

        DateTime dateTime1 = ElasticsearchUtils.parseIndexDate(tableIndex1);

        TableIndexMetadata tableIndexMetadata1 = TableIndexMetadata.builder()
                .date(new Date(dateTime1))
                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(tableIndex1))
                .indexName(tableIndex1)
                .table(ElasticsearchUtils.getTableNameFromIndex(tableIndex1))
                .shardCount(50)
                .noOfEvents(1000000L)
                .noOfColumns(500)
                .averageEventSizeInBytes(20000)
                .averageEventSizeInKBs(StorageSizeUtils.bytesToKiloBytes(20000))
                .averageEventSizeInMBs(StorageSizeUtils.bytesToMegaBytes(20000))
                .averageShardSizeInBytes(500000)
                .averageShardSizeInMBs(StorageSizeUtils.bytesToMegaBytes(500000))
                .averageShardSizeInGBs(StorageSizeUtils.bytesToGigaBytes(500000))
                .totalIndexSizeInBytes(50 * 500000)
                .totalIndexSizeInTBs(StorageSizeUtils.bytesToTeraBytes(50 * 500000))
                .totalIndexSizeInGBs(StorageSizeUtils.bytesToGigaBytes(50 * 500000))
                .totalIndexSizeInMBs(StorageSizeUtils.bytesToMegaBytes(50 * 500000))
                .timestamp(dateTime1.getMillis())
                .updatedAt(currentTime.getMillis())
                .build();

        String tableIndex2 = "foxtrot-payment-table-05-10-2021";

        DateTime dateTime2 = ElasticsearchUtils.parseIndexDate(tableIndex2);

        TableIndexMetadata tableIndexMetadata2 = TableIndexMetadata.builder()
                .date(new Date(dateTime2))
                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(tableIndex2))
                .indexName(tableIndex2)
                .table(ElasticsearchUtils.getTableNameFromIndex(tableIndex2))
                .shardCount(50)
                .noOfEvents(1000000L)
                .noOfColumns(500)
                .averageEventSizeInBytes(20000)
                .averageEventSizeInKBs(StorageSizeUtils.bytesToKiloBytes(20000))
                .averageEventSizeInMBs(StorageSizeUtils.bytesToMegaBytes(20000))
                .averageShardSizeInBytes(500000)
                .averageShardSizeInMBs(StorageSizeUtils.bytesToMegaBytes(500000))
                .averageShardSizeInGBs(StorageSizeUtils.bytesToGigaBytes(500000))
                .totalIndexSizeInBytes(50 * 500000)
                .totalIndexSizeInTBs(StorageSizeUtils.bytesToTeraBytes(50 * 500000))
                .totalIndexSizeInGBs(StorageSizeUtils.bytesToGigaBytes(50 * 500000))
                .totalIndexSizeInMBs(StorageSizeUtils.bytesToMegaBytes(50 * 500000))
                .timestamp(dateTime2.getMillis())
                .updatedAt(currentTime.getMillis())
                .build();

        Map<String, TableIndexMetadata> indicesMetadata = new HashMap<>();
        indicesMetadata.put(tableIndexMetadata1.getIndexName(), tableIndexMetadata1);
        indicesMetadata.put(tableIndexMetadata2.getIndexName(), tableIndexMetadata2);

        indexMetadataMapStore.storeAll(indicesMetadata);

        Set<String> indices = indexMetadataMapStore.loadAllKeys();

        Map<String, TableIndexMetadata> storedIndicesMetadata = indexMetadataMapStore.loadAll(indices);

        Assert.assertEquals(indicesMetadata, storedIndicesMetadata);
    }


    @Test
    public void shouldDeleteAllIndicesMetadata() {
        String tableIndex1 = "foxtrot-payment-table-04-10-2021";
        DateTime currentTime = new DateTime();

        DateTime dateTime1 = ElasticsearchUtils.parseIndexDate(tableIndex1);

        TableIndexMetadata tableIndexMetadata1 = TableIndexMetadata.builder()
                .date(new Date(dateTime1))
                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(tableIndex1))
                .indexName(tableIndex1)
                .table(ElasticsearchUtils.getTableNameFromIndex(tableIndex1))
                .shardCount(50)
                .noOfEvents(1000000L)
                .noOfColumns(500)
                .averageEventSizeInBytes(20000)
                .averageEventSizeInKBs(StorageSizeUtils.bytesToKiloBytes(20000))
                .averageEventSizeInMBs(StorageSizeUtils.bytesToMegaBytes(20000))
                .averageShardSizeInBytes(500000)
                .averageShardSizeInMBs(StorageSizeUtils.bytesToMegaBytes(500000))
                .averageShardSizeInGBs(StorageSizeUtils.bytesToGigaBytes(500000))
                .totalIndexSizeInBytes(50 * 500000)
                .totalIndexSizeInTBs(StorageSizeUtils.bytesToTeraBytes(50 * 500000))
                .totalIndexSizeInGBs(StorageSizeUtils.bytesToGigaBytes(50 * 500000))
                .totalIndexSizeInMBs(StorageSizeUtils.bytesToMegaBytes(50 * 500000))
                .timestamp(dateTime1.getMillis())
                .updatedAt(currentTime.getMillis())
                .build();

        String tableIndex2 = "foxtrot-payment-table-05-10-2021";

        DateTime dateTime2 = ElasticsearchUtils.parseIndexDate(tableIndex2);

        TableIndexMetadata tableIndexMetadata2 = TableIndexMetadata.builder()
                .date(new Date(dateTime2))
                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(tableIndex2))
                .indexName(tableIndex2)
                .table(ElasticsearchUtils.getTableNameFromIndex(tableIndex2))
                .shardCount(50)
                .noOfEvents(1000000L)
                .noOfColumns(500)
                .averageEventSizeInBytes(20000)
                .averageEventSizeInKBs(StorageSizeUtils.bytesToKiloBytes(20000))
                .averageEventSizeInMBs(StorageSizeUtils.bytesToMegaBytes(20000))
                .averageShardSizeInBytes(500000)
                .averageShardSizeInMBs(StorageSizeUtils.bytesToMegaBytes(500000))
                .averageShardSizeInGBs(StorageSizeUtils.bytesToGigaBytes(500000))
                .totalIndexSizeInBytes(50 * 500000)
                .totalIndexSizeInTBs(StorageSizeUtils.bytesToTeraBytes(50 * 500000))
                .totalIndexSizeInGBs(StorageSizeUtils.bytesToGigaBytes(50 * 500000))
                .totalIndexSizeInMBs(StorageSizeUtils.bytesToMegaBytes(50 * 500000))
                .timestamp(dateTime2.getMillis())
                .updatedAt(currentTime.getMillis())
                .build();

        Map<String, TableIndexMetadata> indicesMetadata = new HashMap<>();
        indicesMetadata.put(tableIndexMetadata1.getIndexName(), tableIndexMetadata1);
        indicesMetadata.put(tableIndexMetadata2.getIndexName(), tableIndexMetadata2);

        indexMetadataMapStore.storeAll(indicesMetadata);

        Set<String> indices = indexMetadataMapStore.loadAllKeys();

        Map<String, TableIndexMetadata> storedIndicesMetadata = indexMetadataMapStore.loadAll(indices);

        Assert.assertEquals(indicesMetadata, storedIndicesMetadata);

        indexMetadataMapStore.deleteAll(indices);
        indices = indexMetadataMapStore.loadAllKeys();
        Assert.assertEquals(0, indices.size());

    }
}
