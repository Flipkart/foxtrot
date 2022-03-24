package com.flipkart.foxtrot.core.indexmeta.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadataAttributes;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.util.StorageSizeUtils;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.assertj.core.util.Lists;
import org.joda.time.DateTime;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.when;

public class IndexMetadataManagerImplTest {

    private static HazelcastConnection hazelcastConnection;

    private static ElasticsearchConnection elasticsearchConnection;
    private static IndexMetadataManager indexMetadataManager;

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
        indexMetadataManager = new IndexMetadataManagerImpl(hazelcastConnection, elasticsearchConnection);
        indexMetadataManager.start();

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
    public void shouldSaveIndexMetadata() throws Exception {

        String tableIndex = "foxtrot-payment-table-04-10-2021";
        DateTime currentTime = new DateTime();

        DateTime dateTime = ElasticsearchUtils.parseIndexDate(tableIndex);

        TableIndexMetadata tableIndexMetadata = TableIndexMetadata.builder()
                .date(new Date(dateTime))
                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(tableIndex))
                .indexName(tableIndex)
                .table(ElasticsearchUtils.getTableNameFromIndex(tableIndex))
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
                .timestamp(dateTime.getMillis())
                .updatedAt(currentTime.getMillis())
                .build();

        indexMetadataManager.save(tableIndexMetadata);

        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.getByTable("payment");

        Assert.assertEquals(1, paymentIndicesMetadata.size());
        Assert.assertEquals(tableIndexMetadata, paymentIndicesMetadata.get(0));
    }

    @Test
    public void shouldListIndicesMetadata() throws Exception {

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

        indexMetadataManager.save(tableIndexMetadata1);

        indexMetadataManager.save(tableIndexMetadata2);

        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.getByTable("payment");

        Assert.assertEquals(2, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata2));

    }

    @Test
    public void shouldDeleteIndicesMetadata() throws Exception {
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

        indexMetadataManager.save(tableIndexMetadata1);

        indexMetadataManager.save(tableIndexMetadata2);

        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.getByTable("payment");

        Assert.assertEquals(2, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata2));

        indexMetadataManager.delete(tableIndex1);
        paymentIndicesMetadata = indexMetadataManager.getByTable("payment");
        Assert.assertEquals(1, paymentIndicesMetadata.size());
        Assert.assertFalse(paymentIndicesMetadata.contains(tableIndexMetadata1));
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata2));


    }

    @Test
    public void shouldGetAllIndicesMetadata() throws Exception {

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

        indexMetadataManager.save(tableIndexMetadata1);

        indexMetadataManager.save(tableIndexMetadata2);

        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.getAll();

        Assert.assertEquals(2, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata2));


    }

    @Test
    public void shouldUpdateIndexMetadata() throws Exception {
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

        indexMetadataManager.save(tableIndexMetadata1);

        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.getByTable("payment");

        Assert.assertEquals(1, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));

        tableIndexMetadata1.setShardCount(100);
        indexMetadataManager.update(tableIndex1, tableIndexMetadata1);
        paymentIndicesMetadata = indexMetadataManager.getByTable("payment");

        Assert.assertEquals(1, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));
    }


    @Test
    public void shouldGetIndexMetadataViaFilterRequest() throws Exception {
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

        indexMetadataManager.save(tableIndexMetadata1);

        EqualsFilter equalsFilter = EqualsFilter.builder()
                .field(TableIndexMetadataAttributes.TABLE)
                .value("payment")
                .build();
        List<TableIndexMetadata> paymentIndicesMetadata = indexMetadataManager.search(Lists.newArrayList(equalsFilter));

        Assert.assertEquals(1, paymentIndicesMetadata.size());
        Assert.assertTrue(paymentIndicesMetadata.contains(tableIndexMetadata1));

    }

}
