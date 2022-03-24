package com.flipkart.foxtrot.core.indexmeta.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadataAttributes;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse.ShardType;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.FORMATTER;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableIndexMetadataServiceImplTest {

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
    public void shouldSyncIndexMetadata() {
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();

        tableIndicesInfo.add(indexInfoResponse1);
        tableIndicesInfo.add(indexInfoResponse2);
        tableIndicesInfo.add(indexInfoResponse3);
        tableIndicesInfo.add(indexInfoResponse4);
        tableIndicesInfo.add(indexInfoResponse5);

        Mockito.when(queryStore.getTableIndicesInfo())
                .thenReturn(tableIndicesInfo);

        List<ShardInfoResponse> tableShardsInfo = new ArrayList<>();

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        Mockito.when(queryStore.getTableShardsInfo())
                .thenReturn(tableShardsInfo);

        Mockito.when(tableMetadataManager.getColumnCount(Mockito.anyString()))
                .thenReturn(1000L);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        tableIndexMetadataService.syncTableIndexMetadata(15);

        List<TableIndexMetadata> allIndicesMetadata = tableIndexMetadataService.getAllIndicesMetadata();

        Assert.assertEquals(5, allIndicesMetadata.size());

        Assert.assertTrue(allIndicesMetadata.stream()
                .anyMatch(tableIndexMetadata -> {
                    TableIndexMetadata expectedIndexMetadata = TableIndexMetadata.builder()
                            .indexName("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                            .table("payment")
                            .datePostFix(FORMATTER.print(dateTime1.getMillis()))
                            .shardCount(1)
                            .totalIndexSizeInTBs(0.0)
                            .totalIndexSizeInGBs(0.0)
                            .totalIndexSizeInMBs(1.0)
                            .totalIndexSizeInBytes(1000000)
                            .averageShardSizeInGBs(0.0)
                            .averageShardSizeInMBs(1.0)
                            .averageShardSizeInBytes(1000000)
                            .noOfColumns(1000)
                            .noOfEvents(1000)
                            .averageEventSizeInBytes(1000.0)
                            .averageEventSizeInKBs(1.0)
                            .averageEventSizeInMBs(0.0)
                            .build();

                    return tableIndexMetadata.getIndexName()
                            .equals(expectedIndexMetadata.getIndexName()) && tableIndexMetadata.getTable()
                            .equals(expectedIndexMetadata.getTable()) && tableIndexMetadata.getDatePostFix()
                            .equals(expectedIndexMetadata.getDatePostFix())
                            && tableIndexMetadata.getShardCount() == expectedIndexMetadata.getShardCount()
                            && tableIndexMetadata.getTotalIndexSizeInBytes()
                            == expectedIndexMetadata.getTotalIndexSizeInBytes()
                            && tableIndexMetadata.getTotalIndexSizeInGBs()
                            == expectedIndexMetadata.getTotalIndexSizeInGBs()
                            && tableIndexMetadata.getTotalIndexSizeInTBs()
                            == expectedIndexMetadata.getTotalIndexSizeInTBs()
                            && tableIndexMetadata.getTotalIndexSizeInMBs()
                            == expectedIndexMetadata.getTotalIndexSizeInMBs()
                            && tableIndexMetadata.getNoOfColumns() == expectedIndexMetadata.getNoOfColumns()
                            && tableIndexMetadata.getAverageEventSizeInBytes()
                            == expectedIndexMetadata.getAverageEventSizeInBytes()
                            && tableIndexMetadata.getAverageEventSizeInKBs()
                            == expectedIndexMetadata.getAverageEventSizeInKBs()
                            && tableIndexMetadata.getAverageEventSizeInMBs()
                            == expectedIndexMetadata.getAverageEventSizeInMBs();
                }));

        Assert.assertTrue(allIndicesMetadata.stream()
                .anyMatch(tableIndexMetadata -> {
                    TableIndexMetadata expectedIndexMetadata = TableIndexMetadata.builder()
                            .indexName("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                            .table("payment")
                            .datePostFix(FORMATTER.print(dateTime2.getMillis()))
                            .shardCount(1)
                            .totalIndexSizeInGBs(0.0)
                            .totalIndexSizeInTBs(0.0)
                            .totalIndexSizeInMBs(1.0)
                            .totalIndexSizeInBytes(1000000)
                            .averageShardSizeInGBs(0.0)
                            .averageShardSizeInMBs(1.0)
                            .averageShardSizeInBytes(1000000)
                            .noOfColumns(1000)
                            .noOfEvents(1000)
                            .averageEventSizeInBytes(1000.0)
                            .averageEventSizeInKBs(1.0)
                            .averageEventSizeInMBs(0.0)
                            .build();

                    return tableIndexMetadata.getIndexName()
                            .equals(expectedIndexMetadata.getIndexName()) && tableIndexMetadata.getTable()
                            .equals(expectedIndexMetadata.getTable()) && tableIndexMetadata.getDatePostFix()
                            .equals(expectedIndexMetadata.getDatePostFix())
                            && tableIndexMetadata.getShardCount() == expectedIndexMetadata.getShardCount()
                            && tableIndexMetadata.getTotalIndexSizeInBytes()
                            == expectedIndexMetadata.getTotalIndexSizeInBytes()
                            && tableIndexMetadata.getTotalIndexSizeInTBs()
                            == expectedIndexMetadata.getTotalIndexSizeInTBs()
                            && tableIndexMetadata.getTotalIndexSizeInGBs()
                            == expectedIndexMetadata.getTotalIndexSizeInGBs()
                            && tableIndexMetadata.getTotalIndexSizeInMBs()
                            == expectedIndexMetadata.getTotalIndexSizeInMBs()
                            && tableIndexMetadata.getNoOfColumns() == expectedIndexMetadata.getNoOfColumns()
                            && tableIndexMetadata.getAverageEventSizeInBytes()
                            == expectedIndexMetadata.getAverageEventSizeInBytes()
                            && tableIndexMetadata.getAverageEventSizeInKBs()
                            == expectedIndexMetadata.getAverageEventSizeInKBs()
                            && tableIndexMetadata.getAverageEventSizeInMBs()
                            == expectedIndexMetadata.getAverageEventSizeInMBs();
                }));

        Assert.assertTrue(allIndicesMetadata.stream()
                .anyMatch(tableIndexMetadata -> {
                    TableIndexMetadata expectedIndexMetadata = TableIndexMetadata.builder()
                            .indexName("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                            .table("payment")
                            .datePostFix(FORMATTER.print(dateTime3.getMillis()))
                            .shardCount(1)
                            .totalIndexSizeInTBs(0.0)
                            .totalIndexSizeInGBs(0.0)
                            .totalIndexSizeInMBs(1.0)
                            .totalIndexSizeInBytes(1000000)
                            .averageShardSizeInGBs(0.0)
                            .averageShardSizeInMBs(1.0)
                            .averageShardSizeInBytes(1000000)
                            .noOfColumns(1000)
                            .noOfEvents(1000)
                            .averageEventSizeInBytes(1000.0)
                            .averageEventSizeInKBs(1.0)
                            .averageEventSizeInMBs(0.0)
                            .build();

                    return tableIndexMetadata.getIndexName()
                            .equals(expectedIndexMetadata.getIndexName()) && tableIndexMetadata.getTable()
                            .equals(expectedIndexMetadata.getTable()) && tableIndexMetadata.getDatePostFix()
                            .equals(expectedIndexMetadata.getDatePostFix())
                            && tableIndexMetadata.getShardCount() == expectedIndexMetadata.getShardCount()
                            && tableIndexMetadata.getTotalIndexSizeInBytes()
                            == expectedIndexMetadata.getTotalIndexSizeInBytes()
                            && tableIndexMetadata.getTotalIndexSizeInTBs()
                            == expectedIndexMetadata.getTotalIndexSizeInTBs()
                            && tableIndexMetadata.getTotalIndexSizeInGBs()
                            == expectedIndexMetadata.getTotalIndexSizeInGBs()
                            && tableIndexMetadata.getTotalIndexSizeInMBs()
                            == expectedIndexMetadata.getTotalIndexSizeInMBs()
                            && tableIndexMetadata.getNoOfColumns() == expectedIndexMetadata.getNoOfColumns()
                            && tableIndexMetadata.getAverageEventSizeInBytes()
                            == expectedIndexMetadata.getAverageEventSizeInBytes()
                            && tableIndexMetadata.getAverageEventSizeInKBs()
                            == expectedIndexMetadata.getAverageEventSizeInKBs()
                            && tableIndexMetadata.getAverageEventSizeInMBs()
                            == expectedIndexMetadata.getAverageEventSizeInMBs();
                }));

        Assert.assertTrue(allIndicesMetadata.stream()
                .anyMatch(tableIndexMetadata -> {
                    TableIndexMetadata expectedIndexMetadata = TableIndexMetadata.builder()
                            .indexName("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                            .table("payment")
                            .datePostFix(FORMATTER.print(dateTime4.getMillis()))
                            .shardCount(1)
                            .totalIndexSizeInTBs(0.0)
                            .totalIndexSizeInGBs(0.0)
                            .totalIndexSizeInMBs(1.0)
                            .totalIndexSizeInBytes(1000000)
                            .averageShardSizeInGBs(0.0)
                            .averageShardSizeInMBs(1.0)
                            .averageShardSizeInBytes(1000000)
                            .noOfColumns(1000)
                            .noOfEvents(1000)
                            .averageEventSizeInBytes(1000.0)
                            .averageEventSizeInKBs(1.0)
                            .averageEventSizeInMBs(0.0)
                            .build();

                    return tableIndexMetadata.getIndexName()
                            .equals(expectedIndexMetadata.getIndexName()) && tableIndexMetadata.getTable()
                            .equals(expectedIndexMetadata.getTable()) && tableIndexMetadata.getDatePostFix()
                            .equals(expectedIndexMetadata.getDatePostFix())
                            && tableIndexMetadata.getShardCount() == expectedIndexMetadata.getShardCount()
                            && tableIndexMetadata.getTotalIndexSizeInBytes()
                            == expectedIndexMetadata.getTotalIndexSizeInBytes()
                            && tableIndexMetadata.getTotalIndexSizeInTBs()
                            == expectedIndexMetadata.getTotalIndexSizeInTBs()
                            && tableIndexMetadata.getTotalIndexSizeInGBs()
                            == expectedIndexMetadata.getTotalIndexSizeInGBs()
                            && tableIndexMetadata.getTotalIndexSizeInMBs()
                            == expectedIndexMetadata.getTotalIndexSizeInMBs()
                            && tableIndexMetadata.getNoOfColumns() == expectedIndexMetadata.getNoOfColumns()
                            && tableIndexMetadata.getAverageEventSizeInBytes()
                            == expectedIndexMetadata.getAverageEventSizeInBytes()
                            && tableIndexMetadata.getAverageEventSizeInKBs()
                            == expectedIndexMetadata.getAverageEventSizeInKBs()
                            && tableIndexMetadata.getAverageEventSizeInMBs()
                            == expectedIndexMetadata.getAverageEventSizeInMBs();
                }));

        Assert.assertTrue(allIndicesMetadata.stream()
                .anyMatch(tableIndexMetadata -> {
                    TableIndexMetadata expectedIndexMetadata = TableIndexMetadata.builder()
                            .indexName("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                            .table("payment")
                            .datePostFix(FORMATTER.print(dateTime5.getMillis()))
                            .shardCount(1)
                            .totalIndexSizeInTBs(0.0)
                            .totalIndexSizeInGBs(0.0)
                            .totalIndexSizeInMBs(1.0)
                            .totalIndexSizeInBytes(1000000)
                            .averageShardSizeInGBs(0.0)
                            .averageShardSizeInMBs(1.0)
                            .averageShardSizeInBytes(1000000)
                            .noOfColumns(1000)
                            .noOfEvents(1000)
                            .averageEventSizeInBytes(1000.0)
                            .averageEventSizeInKBs(1.0)
                            .averageEventSizeInMBs(0.0)
                            .build();

                    return tableIndexMetadata.getIndexName()
                            .equals(expectedIndexMetadata.getIndexName()) && tableIndexMetadata.getTable()
                            .equals(expectedIndexMetadata.getTable()) && tableIndexMetadata.getDatePostFix()
                            .equals(expectedIndexMetadata.getDatePostFix())
                            && tableIndexMetadata.getShardCount() == expectedIndexMetadata.getShardCount()
                            && tableIndexMetadata.getTotalIndexSizeInBytes()
                            == expectedIndexMetadata.getTotalIndexSizeInBytes()
                            && tableIndexMetadata.getTotalIndexSizeInTBs()
                            == expectedIndexMetadata.getTotalIndexSizeInTBs()
                            && tableIndexMetadata.getTotalIndexSizeInGBs()
                            == expectedIndexMetadata.getTotalIndexSizeInGBs()
                            && tableIndexMetadata.getTotalIndexSizeInMBs()
                            == expectedIndexMetadata.getTotalIndexSizeInMBs()
                            && tableIndexMetadata.getNoOfColumns() == expectedIndexMetadata.getNoOfColumns()
                            && tableIndexMetadata.getAverageEventSizeInBytes()
                            == expectedIndexMetadata.getAverageEventSizeInBytes()
                            && tableIndexMetadata.getAverageEventSizeInKBs()
                            == expectedIndexMetadata.getAverageEventSizeInKBs()
                            && tableIndexMetadata.getAverageEventSizeInMBs()
                            == expectedIndexMetadata.getAverageEventSizeInMBs();
                }));

    }

    @Test
    public void shouldDeleteOldMetadata() {
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        Mockito.when(tableMetadataManager.get())
                .thenReturn(Lists.newArrayList(Table.builder()
                        .name("payment")
                        .tenantName("PAYMENT")
                        .shards(1)
                        .columns(5000)
                        .defaultRegions(4)
                        .seggregatedBackend(true)
                        .refreshIntervalInSecs(30)
                        .ttl(10)
                        .build()));
        QueryStore queryStore = Mockito.mock(QueryStore.class);

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();

        tableIndicesInfo.add(indexInfoResponse1);
        tableIndicesInfo.add(indexInfoResponse2);
        tableIndicesInfo.add(indexInfoResponse3);
        tableIndicesInfo.add(indexInfoResponse4);
        tableIndicesInfo.add(indexInfoResponse5);

        Mockito.when(queryStore.getTableIndicesInfo())
                .thenReturn(tableIndicesInfo);

        List<ShardInfoResponse> tableShardsInfo = new ArrayList<>();

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        Mockito.when(queryStore.getTableShardsInfo())
                .thenReturn(tableShardsInfo);

        Mockito.when(tableMetadataManager.getColumnCount(Mockito.anyString()))
                .thenReturn(1000L);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        tableIndexMetadataService.syncTableIndexMetadata(15);

        List<TableIndexMetadata> allIndicesMetadata = tableIndexMetadataService.getAllIndicesMetadata();

        Assert.assertEquals(5, allIndicesMetadata.size());

        tableIndexMetadataService.cleanupIndexMetadata(2);
        allIndicesMetadata = tableIndexMetadataService.getAllIndicesMetadata();

        Assert.assertEquals(2, allIndicesMetadata.size());
    }

    @Test
    public void shouldGetIndexMetadataByIndexName() {
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();

        tableIndicesInfo.add(indexInfoResponse1);
        tableIndicesInfo.add(indexInfoResponse2);
        tableIndicesInfo.add(indexInfoResponse3);
        tableIndicesInfo.add(indexInfoResponse4);
        tableIndicesInfo.add(indexInfoResponse5);

        Mockito.when(queryStore.getTableIndicesInfo())
                .thenReturn(tableIndicesInfo);

        List<ShardInfoResponse> tableShardsInfo = new ArrayList<>();

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        Mockito.when(queryStore.getTableShardsInfo())
                .thenReturn(tableShardsInfo);

        Mockito.when(tableMetadataManager.getColumnCount(Mockito.anyString()))
                .thenReturn(1000L);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        tableIndexMetadataService.syncTableIndexMetadata(15);

        TableIndexMetadata indexMetadata = tableIndexMetadataService.getIndexMetadata(
                "foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()));
        Assert.assertNotNull(indexMetadata);
    }

    @Test
    public void shouldSearchIndexMetadata() {
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();

        tableIndicesInfo.add(indexInfoResponse1);
        tableIndicesInfo.add(indexInfoResponse2);
        tableIndicesInfo.add(indexInfoResponse3);
        tableIndicesInfo.add(indexInfoResponse4);
        tableIndicesInfo.add(indexInfoResponse5);

        Mockito.when(queryStore.getTableIndicesInfo())
                .thenReturn(tableIndicesInfo);

        List<ShardInfoResponse> tableShardsInfo = new ArrayList<>();

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        Mockito.when(queryStore.getTableShardsInfo())
                .thenReturn(tableShardsInfo);

        Mockito.when(tableMetadataManager.getColumnCount(Mockito.anyString()))
                .thenReturn(1000L);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        tableIndexMetadataService.syncTableIndexMetadata(15);

        List<TableIndexMetadata> paymentTableIndicesMetadata = tableIndexMetadataService.searchIndexMetadata(
                Lists.newArrayList(EqualsFilter.builder()
                        .field(TableIndexMetadataAttributes.TABLE)
                        .value("payment")
                        .build()));

        Assert.assertEquals(5, paymentTableIndicesMetadata.size());
    }

    @Test
    public void shouldGetIndexMetadataByTableName() {
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000)
                .primaryStoreSize(1000000L)
                .health("healthy")
                .status("STARTED")
                .build();

        tableIndicesInfo.add(indexInfoResponse1);
        tableIndicesInfo.add(indexInfoResponse2);
        tableIndicesInfo.add(indexInfoResponse3);
        tableIndicesInfo.add(indexInfoResponse4);
        tableIndicesInfo.add(indexInfoResponse5);

        Mockito.when(queryStore.getTableIndicesInfo())
                .thenReturn(tableIndicesInfo);

        List<ShardInfoResponse> tableShardsInfo = new ArrayList<>();

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.PRIMARY.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());
        tableShardsInfo.add(ShardInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .shard("0")
                .primaryOrReplica(ShardType.REPLICA.getName())
                .node("elasticsearch101.com")
                .state("STARTED")
                .build());

        Mockito.when(queryStore.getTableShardsInfo())
                .thenReturn(tableShardsInfo);

        Mockito.when(tableMetadataManager.getColumnCount(Mockito.anyString()))
                .thenReturn(1000L);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        tableIndexMetadataService.syncTableIndexMetadata(15);

        List<TableIndexMetadata> indexMetadata = tableIndexMetadataService.getTableIndicesMetadata("payment");
        Assert.assertEquals(5, indexMetadata.size());
    }

}
