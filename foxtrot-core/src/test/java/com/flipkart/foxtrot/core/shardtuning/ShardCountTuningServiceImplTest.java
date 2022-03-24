package com.flipkart.foxtrot.core.shardtuning;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetailResponse;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetails;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetails.DiskUsageInfo;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.impl.IndexMetadataManagerImpl;
import com.flipkart.foxtrot.core.indexmeta.impl.TableIndexMetadataServiceImpl;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse.ShardType;
import com.flipkart.foxtrot.core.table.TableManager;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.FORMATTER;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ShardCountTuningServiceImplTest {

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
    public void shouldTuneShardCountToMaxShardCapacityOfNodeGroup() {
        ShardCountTuningJobConfig shardCountTuningJobConfig = new ShardCountTuningJobConfig();
        shardCountTuningJobConfig.setReplicationFactor(1);
        shardCountTuningJobConfig.setIdealShardSizeInGBs(2);
        shardCountTuningJobConfig.setRollingWindowInDays(5);
        shardCountTuningJobConfig.setShardCountTuningEnabled(true);

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        populateIndexMetadata(tableMetadataManager, queryStore, tableIndexMetadataService);

        TableManager tableManager = new InMemoryTableManager();
        tableManager.save(Table.builder()
                .name("payment")
                .tenantName("PAYMENT")
                .shards(1)
                .columns(5000)
                .defaultRegions(4)
                .seggregatedBackend(true)
                .refreshIntervalInSecs(30)
                .ttl(10)
                .build());

        NodeGroupManager nodeGroupManager = Mockito.mock(NodeGroupManager.class);

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(
                        new TreeSet<>(Lists.newArrayList("elasticsearch10*", "elasticsearch20*", "elasticsearch30*")))
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(2)
                        .tables(new TreeSet<>(Lists.newArrayList("payment")))
                        .build())
                .build();

        Mockito.when(nodeGroupManager.getNodeGroupByTable("payment"))
                .thenReturn(paymentGroup);

        TreeMap<String, DiskUsageInfo> nodeInfoMap = new TreeMap<String, DiskUsageInfo>();
        nodeInfoMap.put("elasticsearch101.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch102.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch201.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch202.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch301.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch302.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());

        Mockito.when(nodeGroupManager.getNodeGroupDetails("payment"))
                .thenReturn(ESNodeGroupDetailResponse.builder()
                        .nodeGroup(paymentGroup)
                        .details(ESNodeGroupDetails.builder()
                                .nodeCount(6)
                                .diskUsageInfo(DiskUsageInfo.builder()
                                        .availableDiskStorage("600G")
                                        .totalDiskStorage("1200G")
                                        .usedDiskPercentage("50%")
                                        .usedDiskStorage("600G")
                                        .build())
                                .nodeInfo(nodeInfoMap)
                                .build())
                        .build());

        EventBusManager eventBusManager = Mockito.mock(EventBusManager.class);

        ShardCountTuningService shardCountTuningService = new ShardCountTuningServiceImpl(shardCountTuningJobConfig,
                tableIndexMetadataService, tableManager, eventBusManager, nodeGroupManager);

        shardCountTuningService.tuneShardCount();

        Table updatedTable = tableManager.get("payment");

        Assert.assertEquals(12, updatedTable.getShards());
    }

    @Test
    public void shouldTuneShardCount() {
        ShardCountTuningJobConfig shardCountTuningJobConfig = new ShardCountTuningJobConfig();
        shardCountTuningJobConfig.setReplicationFactor(1);
        shardCountTuningJobConfig.setIdealShardSizeInGBs(10);
        shardCountTuningJobConfig.setRollingWindowInDays(5);
        shardCountTuningJobConfig.setShardCountTuningEnabled(true);

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);

        QueryStore queryStore = Mockito.mock(QueryStore.class);

        TableIndexMetadataService tableIndexMetadataService = new TableIndexMetadataServiceImpl(tableMetadataManager,
                indexMetadataManager, queryStore);

        populateIndexMetadata(tableMetadataManager, queryStore, tableIndexMetadataService);

        TableManager tableManager = new InMemoryTableManager();
        tableManager.save(Table.builder()
                .name("payment")
                .tenantName("PAYMENT")
                .shards(1)
                .columns(5000)
                .defaultRegions(4)
                .seggregatedBackend(true)
                .refreshIntervalInSecs(30)
                .ttl(10)
                .build());

        NodeGroupManager nodeGroupManager = Mockito.mock(NodeGroupManager.class);

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(
                        new TreeSet<>(Lists.newArrayList("elasticsearch10*", "elasticsearch20*", "elasticsearch30*")))
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(2)
                        .tables(new TreeSet<>(Lists.newArrayList("payment")))
                        .build())
                .build();

        Mockito.when(nodeGroupManager.getNodeGroupByTable("payment"))
                .thenReturn(paymentGroup);

        TreeMap<String, DiskUsageInfo> nodeInfoMap = new TreeMap<String, DiskUsageInfo>();
        nodeInfoMap.put("elasticsearch101.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch102.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch201.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch202.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch301.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());
        nodeInfoMap.put("elasticsearch302.com", DiskUsageInfo.builder()
                .availableDiskStorage("100G")
                .totalDiskStorage("200G")
                .usedDiskPercentage("50%")
                .usedDiskStorage("100G")
                .build());

        Mockito.when(nodeGroupManager.getNodeGroupDetails("payment"))
                .thenReturn(ESNodeGroupDetailResponse.builder()
                        .nodeGroup(paymentGroup)
                        .details(ESNodeGroupDetails.builder()
                                .nodeCount(6)
                                .diskUsageInfo(DiskUsageInfo.builder()
                                        .availableDiskStorage("600G")
                                        .totalDiskStorage("1200G")
                                        .usedDiskPercentage("50%")
                                        .usedDiskStorage("600G")
                                        .build())
                                .nodeInfo(nodeInfoMap)
                                .build())
                        .build());

        EventBusManager eventBusManager = Mockito.mock(EventBusManager.class);

        ShardCountTuningService shardCountTuningService = new ShardCountTuningServiceImpl(shardCountTuningJobConfig,
                tableIndexMetadataService, tableManager, eventBusManager, nodeGroupManager);

        shardCountTuningService.tuneShardCount();

        Table updatedTable = tableManager.get("payment");

        Assert.assertEquals(6, updatedTable.getShards());
    }


    private void populateIndexMetadata(TableMetadataManager tableMetadataManager,
                                       QueryStore queryStore,
                                       TableIndexMetadataService tableIndexMetadataService) {

        DateTime dateTime = new DateTime();

        DateTime dateTime1 = new DateTime(dateTime.minusDays(1));
        DateTime dateTime2 = new DateTime(dateTime.minusDays(2));
        DateTime dateTime3 = new DateTime(dateTime.minusDays(3));
        DateTime dateTime4 = new DateTime(dateTime.minusDays(4));
        DateTime dateTime5 = new DateTime(dateTime.minusDays(5));

        List<IndexInfoResponse> tableIndicesInfo = new ArrayList<>();
        IndexInfoResponse indexInfoResponse1 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime1.getMillis()))
                .docCount(1000000)
                .primaryStoreSize(53687091200L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse2 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime2.getMillis()))
                .docCount(1000000)
                .primaryStoreSize(53687091200L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse3 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime3.getMillis()))
                .docCount(1000000)
                .primaryStoreSize(53687091200L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse4 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime4.getMillis()))
                .docCount(1000000)
                .primaryStoreSize(53687091200L)
                .health("healthy")
                .status("STARTED")
                .build();
        IndexInfoResponse indexInfoResponse5 = IndexInfoResponse.builder()
                .index("foxtrot-payment-table-" + FORMATTER.print(dateTime5.getMillis()))
                .docCount(1000000)
                .primaryStoreSize(53687091200L)
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

        tableIndexMetadataService.syncTableIndexMetadata(15);
    }

    private static class InMemoryTableManager implements TableManager {

        private Map<String, Table> tables;

        public InMemoryTableManager() {
            tables = new ConcurrentHashMap<>();
        }

        @Override
        public void save(Table table) {
            tables.put(table.getName(), table);
        }

        @Override
        public void save(Table table,
                         boolean forceCreateTable) {
            tables.put(table.getName(), table);
        }

        @Override
        public Table get(String name) {
            return tables.get(name);
        }

        @Override
        public List<Table> getAll() {
            return new ArrayList<>(tables.values());
        }

        @Override
        public void update(Table table) {
            tables.put(table.getName(), table);
        }

        @Override
        public void delete(String name) {
            tables.remove(name);
        }
    }
}
