package com.flipkart.foxtrot.core.nodegroup;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.FileSystemDetails;
import com.flipkart.foxtrot.common.elasticsearch.node.FileSystemDetails.FileSystemOverview;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse.NodeFSDetails;
import com.flipkart.foxtrot.common.exception.NodeGroupExecutionException;
import com.flipkart.foxtrot.common.nodegroup.*;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetails.DiskUsageInfo;
import com.flipkart.foxtrot.common.nodegroup.visitors.MoveTablesRequest;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.NodeGroupActivityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.nodegroup.repository.InMemoryNodeGroupRepository;
import com.flipkart.foxtrot.core.nodegroup.repository.NodeGroupRepository;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.flipkart.foxtrot.core.util.StorageSizeUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;


public class NodeGroupManagerImplTest {

    private static final List<String> DATA_NODES = Lists.newArrayList("elasticsearch100.test.nmx",
            "elasticsearch101.test.nmx", "elasticsearch200.test.nmx", "elasticsearch201.test.nmx",
            "elasticsearch300.test.nmx", "elasticsearch301.test.nmx", "elasticsearch400.test.nmx",
            "elasticsearch401.test.nmx", "elasticsearch500.test.nmx", "elasticsearch501.test.nmx");
    private static final List<String> INDICES = Lists.newArrayList("foxtrot-payment-table-10-8-2021",
            "foxtrot-test_consumer_app_android-table-10-8-2021", "foxtrot-bullhorn-table-10-8-2021",
            "foxtrot-hermes-table-10-8-2021", "foxtrot-mercedes-table-10-8-2021");
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private NodeGroupManager nodeGroupManager;
    private NodeGroupRepository nodeGroupRepository;
    private RestClient mockLowLevelClient;
    private RestHighLevelClient mockHighLevelClient;
    private AllocationManager allocationManager;

    @Before
    public void beforeMethod() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS);
        SerDe.init(objectMapper);

        mockHighLevelClient = Mockito.mock(RestHighLevelClient.class);
        mockLowLevelClient = Mockito.mock(RestClient.class);
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        Mockito.when(elasticsearchConnection.getClient())
                .thenReturn(mockHighLevelClient);
        Mockito.when(mockHighLevelClient.getLowLevelClient())
                .thenReturn(mockLowLevelClient);

        nodeGroupRepository = new InMemoryNodeGroupRepository();

        allocationManager = Mockito.mock(AllocationManager.class);
        Mockito.doNothing()
                .when(allocationManager)
                .createNodeAllocationTemplate(Mockito.any(AllocatedESNodeGroup.class));
        NodeGroupActivityConfig nodeGroupActivityConfig = NodeGroupActivityConfig.builder()
                .vacantGroupReadRepairIntervalInMins(0)
                .build();

        TableManager tableManager = Mockito.mock(TableManager.class);

        Mockito.when(tableManager.getAll())
                .thenReturn(Lists.newArrayList(Table.builder()
                        .name("payment")
                        .ttl(30)
                        .columns(5000)
                        .defaultRegions(4)
                        .seggregatedBackend(true)
                        .shards(1)
                        .tenantName("INFRA")
                        .build(), Table.builder()
                        .name("test_consumer_app_android")
                        .ttl(30)
                        .columns(5000)
                        .defaultRegions(4)
                        .seggregatedBackend(true)
                        .shards(1)
                        .tenantName("INFRA")
                        .build(), Table.builder()
                        .name("mercury")
                        .ttl(30)
                        .columns(5000)
                        .defaultRegions(4)
                        .seggregatedBackend(true)
                        .shards(1)
                        .tenantName("INFRA")
                        .build()));
        ElasticsearchQueryStore queryStore = new ElasticsearchQueryStore(Mockito.mock(TableMetadataManager.class),
                Mockito.mock(TenantMetadataManager.class), elasticsearchConnection, Mockito.mock(DataStore.class),
                new ArrayList<>(), Mockito.mock(ElasticsearchTemplateMappingParser.class),
                Mockito.mock(CardinalityConfig.class));

        nodeGroupManager = new NodeGroupManagerImpl(nodeGroupRepository, allocationManager, queryStore, tableManager,
                Executors.newFixedThreadPool(4), nodeGroupActivityConfig);
    }

    @Test
    public void shouldCreateNodeGroupWithSpecificAllocation() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(paymentGroup);

        Assert.assertEquals(paymentGroup, nodeGroupManager.getNodeGroup("payment"));

        VacantESNodeGroup vacantGroup = nodeGroupRepository.getVacantGroup();
        Assert.assertNotNull(vacantGroup);
        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch300.test.nmx", "elasticsearch301.test.nmx",
                        "elasticsearch400.test.nmx", "elasticsearch401.test.nmx", "elasticsearch500.test.nmx",
                        "elasticsearch501.test.nmx"));
        Assert.assertEquals(vacantNodes.size(), vacantGroup.getNodePatterns()
                .size());

        Assert.assertEquals(vacantNodes, vacantGroup.getNodePatterns());
    }

    @Test
    public void shouldNotCreateNodeGroupWithOverlappingNodePattern() throws IOException {
        exception.expect(NodeGroupExecutionException.class);
        exception.expectMessage(Matchers.containsString(
                "Node patterns overlapping with existing node group: test_consumer_app_android"));

        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch201*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(paymentGroup);

    }

    @Test
    public void shouldNotCreateNodeGroupWithOverlappingTables() throws IOException {
        exception.expect(NodeGroupExecutionException.class);
        exception.expectMessage(Matchers.containsString(
                "Table allocation overlapping with existing node group: test_consumer_app_android"));

        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("payment");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch3*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(paymentGroup);

    }

    @Test
    public void shouldNotCreateNodeGroupWhenSameGroupNameAlreadyExists() {
        exception.expect(NodeGroupExecutionException.class);
        exception.expectMessage(Matchers.containsString("Node group already exists with given name"));

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        nodeGroupManager.createNodeGroup(androidGroup);
    }


    @Test
    public void shouldCreateNodeGroupWithCommonAllocation() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");
        nodePatterns2.add("elasticsearch3*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns2)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(allGroup);

        Assert.assertEquals(allGroup, nodeGroupManager.getNodeGroup("all"));
        Assert.assertEquals(allGroup, nodeGroupRepository.getCommonGroup());

        VacantESNodeGroup vacantGroup = nodeGroupRepository.getVacantGroup();
        Assert.assertNotNull(vacantGroup);
        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch400.test.nmx", "elasticsearch401.test.nmx",
                        "elasticsearch500.test.nmx", "elasticsearch501.test.nmx"));
        Assert.assertEquals(vacantNodes.size(), vacantGroup.getNodePatterns()
                .size());

        Assert.assertEquals(vacantNodes, vacantGroup.getNodePatterns());
    }

    @Test
    public void shouldNotCreateMultipleNodeGroupWithCommonAllocation() throws IOException {
        exception.expect(NodeGroupExecutionException.class);
        exception.expectMessage(Matchers.containsString("Table allocation overlapping with existing node group: all"));

        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");
        nodePatterns2.add("elasticsearch3*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns2)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(allGroup);

        Assert.assertEquals(allGroup, nodeGroupManager.getNodeGroup("all"));

        SortedSet<String> nodePatterns3 = new TreeSet<>();
        nodePatterns3.add("elasticsearch4*");
        nodePatterns3.add("elasticsearch5*");

        AllocatedESNodeGroup anotherAllGroup = AllocatedESNodeGroup.builder()
                .groupName("anotherAll")
                .nodePatterns(nodePatterns3)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(2)
                        .build())
                .build();

        setupNodeFSDetailsMock();

        nodeGroupManager.createNodeGroup(anotherAllGroup);

    }

    @Test
    public void shouldCreateVacantNodeGroup() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");
        nodePatterns1.add("elasticsearch3*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch400.test.nmx", "elasticsearch401.test.nmx",
                        "elasticsearch500.test.nmx", "elasticsearch501.test.nmx"));

        VacantESNodeGroup vacantESNodeGroup = VacantESNodeGroup.builder()
                .groupName("vacant")
                .nodePatterns(vacantNodes)
                .build();

        nodeGroupManager.createNodeGroup(vacantESNodeGroup);
        setupNodeFSDetailsMock();

        Assert.assertEquals(vacantESNodeGroup, nodeGroupManager.getNodeGroup("vacant"));

        VacantESNodeGroup vacantGroup = nodeGroupRepository.getVacantGroup();
        Assert.assertNotNull(vacantGroup);
        Assert.assertEquals(vacantNodes.size(), vacantGroup.getNodePatterns()
                .size());

        Assert.assertEquals(vacantNodes, vacantGroup.getNodePatterns());

    }

    @Test
    public void shouldGetNodeGroups() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(paymentGroup);

        SortedSet<String> nodePatterns3 = new TreeSet<>();
        nodePatterns3.add("elasticsearch3*");
        nodePatterns3.add("elasticsearch4*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns3)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        setupNodeFSDetailsMock();

        nodeGroupManager.createNodeGroup(allGroup);

        Assert.assertEquals(allGroup, nodeGroupManager.getNodeGroup("all"));

        List<ESNodeGroup> nodeGroups = nodeGroupManager.getNodeGroups();

        Assert.assertEquals(4, nodeGroups.size());
        Assert.assertTrue(nodeGroups.contains(androidGroup));
        Assert.assertTrue(nodeGroups.contains(paymentGroup));
        Assert.assertTrue(nodeGroups.contains(allGroup));

        Optional<ESNodeGroup> vacantNodeGroup = nodeGroups.stream()
                .filter(nodeGroup -> nodeGroup.getStatus()
                        .equals(AllocationStatus.VACANT))
                .findFirst();
        Assert.assertTrue(vacantNodeGroup.isPresent());
        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch500.test.nmx", "elasticsearch501.test.nmx"));
        Assert.assertEquals(vacantNodes.size(), vacantNodeGroup.get()
                .getNodePatterns()
                .size());
        Assert.assertEquals(vacantNodes, vacantNodeGroup.get()
                .getNodePatterns());
    }

    @Test
    public void shouldGetNodeGroupByTable() throws IOException {

        SortedSet<String> nodePatterns3 = new TreeSet<>();
        nodePatterns3.add("elasticsearch3*");
        nodePatterns3.add("elasticsearch4*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns3)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        setupNodeFSDetailsMock();

        nodeGroupManager.createNodeGroup(allGroup);

        Assert.assertEquals(allGroup, nodeGroupManager.getNodeGroup("all"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(paymentGroup);

        ESNodeGroup actualPaymentNodeGroup = nodeGroupManager.getNodeGroupByTable("payment");

        Assert.assertEquals(paymentGroup, actualPaymentNodeGroup);

        ESNodeGroup actualMercuryNodeGroup = nodeGroupManager.getNodeGroupByTable("mercury");

        Assert.assertEquals(allGroup, actualMercuryNodeGroup);

    }


    @Test
    public void shouldGetNodeGroupDetails() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        nodeGroupManager.createNodeGroup(paymentGroup);

        SortedSet<String> nodePatterns3 = new TreeSet<>();
        nodePatterns3.add("elasticsearch3*");
        nodePatterns3.add("elasticsearch4*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns3)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        setupNodeFSDetailsMock();

        nodeGroupManager.createNodeGroup(allGroup);

        setupNodeFSDetailsMock();

        Assert.assertEquals(allGroup, nodeGroupManager.getNodeGroup("all"));

        setupNodeFSDetailsMock();

        List<ESNodeGroupDetailResponse> nodeGroupDetails = nodeGroupManager.getNodeGroupDetails();

        Assert.assertEquals(4, nodeGroupDetails.size());

        ESNodeGroupDetailResponse androidGroupDetails = ESNodeGroupDetailResponse.builder()
                .nodeGroup(androidGroup)
                .details(ESNodeGroupDetails.builder()
                        .nodeCount(2)
                        .nodeInfo(new TreeMap<>(ImmutableMap.of("elasticsearch100.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(),

                                "elasticsearch101.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build())))
                        .diskUsageInfo(DiskUsageInfo.builder()
                                .totalDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9))))
                                .availableDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskPercentage("50.00%")
                                .build())
                        .build())
                .build();

        ESNodeGroupDetailResponse paymentGroupDetails = ESNodeGroupDetailResponse.builder()
                .nodeGroup(paymentGroup)
                .details(ESNodeGroupDetails.builder()
                        .nodeCount(2)
                        .nodeInfo(new TreeMap<>(ImmutableMap.of("elasticsearch200.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(),

                                "elasticsearch201.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build())))
                        .diskUsageInfo(DiskUsageInfo.builder()
                                .totalDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9))))
                                .availableDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskPercentage("50.00%")
                                .build())
                        .build())
                .build();

        ESNodeGroupDetailResponse allGroupDetails = ESNodeGroupDetailResponse.builder()
                .nodeGroup(allGroup)
                .details(ESNodeGroupDetails.builder()
                        .nodeCount(4)
                        .nodeInfo(new TreeMap<>(ImmutableMap.of("elasticsearch300.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(),

                                "elasticsearch301.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(), "elasticsearch400.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(),

                                "elasticsearch401.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build())))
                        .diskUsageInfo(DiskUsageInfo.builder()
                                .totalDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(4 * ((long) Math.pow(10, 9))))
                                .availableDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(4 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(4 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskPercentage("50.00%")
                                .build())
                        .build())
                .build();

        setupNodeFSDetailsMock();
        VacantESNodeGroup vacantESNodeGroup = (VacantESNodeGroup) nodeGroupManager.getNodeGroup("vacant");

        ESNodeGroupDetailResponse vacantGroupDetails = ESNodeGroupDetailResponse.builder()
                .nodeGroup(vacantESNodeGroup)
                .details(ESNodeGroupDetails.builder()
                        .nodeCount(2)
                        .nodeInfo(new TreeMap<>(ImmutableMap.of("elasticsearch500.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build(),

                                "elasticsearch501.test.nmx", DiskUsageInfo.builder()
                                        .totalDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9)))
                                        .availableDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskStorage(
                                                StorageSizeUtils.humanReadableByteCountSI((long) Math.pow(10, 9) / 2))
                                        .usedDiskPercentage("50.00%")
                                        .build())))
                        .diskUsageInfo(DiskUsageInfo.builder()
                                .totalDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9))))
                                .availableDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskStorage(
                                        StorageSizeUtils.humanReadableByteCountSI(2 * ((long) Math.pow(10, 9) / 2)))
                                .usedDiskPercentage("50.00%")
                                .build())
                        .build())
                .build();

        Assert.assertTrue(nodeGroupDetails.contains(androidGroupDetails));
        Assert.assertTrue(nodeGroupDetails.contains(paymentGroupDetails));
        Assert.assertTrue(nodeGroupDetails.contains(allGroupDetails));
        Assert.assertTrue(nodeGroupDetails.contains(vacantGroupDetails));

    }


    @Test
    public void shouldDeleteNodeGroup() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();
        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch2*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(paymentGroup);

        setupNodeFSDetailsMock();
        nodeGroupManager.deleteNodeGroup("payment");

        setupNodeFSDetailsMock();
        VacantESNodeGroup vacantESNodeGroup = (VacantESNodeGroup) nodeGroupManager.getNodeGroup("vacant");

        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch200.test.nmx", "elasticsearch201.test.nmx",
                        "elasticsearch300.test.nmx", "elasticsearch301.test.nmx", "elasticsearch400.test.nmx",
                        "elasticsearch401.test.nmx", "elasticsearch500.test.nmx",
                        "elasticsearch501.test.nmx"));

        Assert.assertEquals(vacantNodes, vacantESNodeGroup.getNodePatterns());
    }

    @Test
    public void shouldUpdateNodePatternInNodeGroup() throws IOException {

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();
        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(androidGroup);

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch3*");

        AllocatedESNodeGroup allGroup = AllocatedESNodeGroup.builder()
                .groupName("all")
                .nodePatterns(nodePatterns2)
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .build())
                .build();

        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(allGroup);

        androidGroup.setNodePatterns(new TreeSet<>(Lists.newArrayList("elasticsearch1*")));
        setupNodeFSDetailsMock();
        setupListIndicesMock();
        nodeGroupManager.updateNodeGroup("test_consumer_app_android", androidGroup);
        await().pollDelay(5000, TimeUnit.MILLISECONDS)
                .until(() -> true);

        Mockito.verify(allocationManager, Mockito.atLeastOnce())
                .syncAllocationSettings(Sets.newHashSet("foxtrot-test_consumer_app_android-table-10-8-2021"),
                        androidGroup);

        Mockito.verify(allocationManager, Mockito.atLeastOnce())
                .syncAllocationSettings(
                        Sets.newHashSet("foxtrot-payment-table-10-8-2021", "foxtrot-bullhorn-table-10-8-2021",
                                "foxtrot-hermes-table-10-8-2021", "foxtrot-mercedes-table-10-8-2021"), allGroup);

        setupNodeFSDetailsMock();
        VacantESNodeGroup vacantGroup = nodeGroupRepository.getVacantGroup();
        Assert.assertNotNull(vacantGroup);
        SortedSet<String> vacantNodes = Sets.newTreeSet(
                Lists.newArrayList("elasticsearch200.test.nmx", "elasticsearch201.test.nmx",
                        "elasticsearch400.test.nmx", "elasticsearch401.test.nmx", "elasticsearch500.test.nmx",
                        "elasticsearch501.test.nmx"));
        Assert.assertEquals(vacantNodes.size(), vacantGroup.getNodePatterns()
                .size());

        Assert.assertEquals(vacantNodes, vacantGroup.getNodePatterns());
    }

    @Test
    public void shouldMoveTablesBetweenGroups() throws IOException {
        setupNodeFSDetailsMock();

        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("bullhorn");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();
        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get("test_consumer_app_android"));

        SortedSet<String> nodePatterns2 = new TreeSet<>();
        nodePatterns2.add("elasticsearch3*");

        SortedSet<String> tables2 = new TreeSet<>();
        tables2.add("payment");

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .groupName("payment")
                .nodePatterns(nodePatterns2)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables2)
                        .build())
                .build();

        setupNodeFSDetailsMock();
        nodeGroupManager.createNodeGroup(paymentGroup);

        setupNodeFSDetailsMock();
        setupListIndicesMock();
        nodeGroupManager.moveTablesBetweenGroups(MoveTablesRequest.builder()
                .sourceGroup("test_consumer_app_android")
                .destinationGroup("payment")
                .tables(Lists.newArrayList("bullhorn"))
                .build());

        setupNodeFSDetailsMock();
        AllocatedESNodeGroup androidGroupUpdated = (AllocatedESNodeGroup) nodeGroupManager.getNodeGroup(
                "test_consumer_app_android");
        Assert.assertTrue(androidGroupUpdated.getTableAllocation() instanceof SpecificTableAllocation);

        SpecificTableAllocation androidTableAllocation = (SpecificTableAllocation) androidGroupUpdated.getTableAllocation();

        Assert.assertFalse(androidTableAllocation.getTables()
                .contains("bullhorn"));

        setupNodeFSDetailsMock();
        AllocatedESNodeGroup paymentGroupUpdated = (AllocatedESNodeGroup) nodeGroupManager.getNodeGroup("payment");
        Assert.assertTrue(paymentGroupUpdated.getTableAllocation() instanceof SpecificTableAllocation);

        SpecificTableAllocation paymentGroupUpdatedTableAllocation = (SpecificTableAllocation) paymentGroupUpdated.getTableAllocation();

        Assert.assertTrue(paymentGroupUpdatedTableAllocation.getTables()
                .contains("bullhorn"));
        await().pollDelay(5000, TimeUnit.MILLISECONDS)
                .until(() -> true);

        Mockito.verify(allocationManager, Mockito.atLeastOnce())
                .syncAllocationSettings(Sets.newHashSet("foxtrot-bullhorn-table-10-8-2021"), paymentGroupUpdated);
    }


    @Test
    public void checkOverlappingNodePatterns() {

        SortedSet<String> groupATables = new TreeSet<>();
        groupATables.add("test_consumer_app_android");
        groupATables.add("pcaa_critical");

        SortedSet<String> groupANodePatterns = new TreeSet<>();
        groupANodePatterns.add("elasticsearch8*");
        groupANodePatterns.add("elasticsearch9*");

        SortedSet<String> groupBTables = new TreeSet<>();
        groupBTables.add("flipcast");
        groupBTables.add("zencast");

        SortedSet<String> groupBNodePatterns = new TreeSet<>();
        groupBNodePatterns.add("elasticsearch81*");
        groupBNodePatterns.add("elasticsearch91*");

        AllocatedESNodeGroup allocatedESNodeGroupA = AllocatedESNodeGroup.builder()
                .groupName("groupA")
                .tableAllocation(SpecificTableAllocation.builder()
                        .tables(groupATables)
                        .totalShardsPerNode(4)
                        .build())
                .nodePatterns(groupANodePatterns)
                .build();

        AllocatedESNodeGroup allocatedESNodeGroupB = AllocatedESNodeGroup.builder()
                .groupName("groupB")
                .tableAllocation(SpecificTableAllocation.builder()
                        .tables(groupBTables)
                        .totalShardsPerNode(4)
                        .build())
                .nodePatterns(groupBNodePatterns)
                .build();

        Assert.assertTrue(allocatedESNodeGroupA.isAnyNodePatternOverlappingWith(allocatedESNodeGroupB));

    }

    private void setupNodeFSDetailsMock() throws IOException {
        Map<String, NodeFSDetails> nodeFSDetailsMap = new HashMap<>();

        IntStream.range(0, 10)
                .forEach(i -> {
                    nodeFSDetailsMap.put(String.valueOf(i), NodeFSDetails.builder()
                            .name(DATA_NODES.get(i))
                            .roles(Lists.newArrayList("ingest", "data"))
                            .fs(FileSystemDetails.builder()
                                    .total(FileSystemOverview.builder()
                                            .totalInBytes((long) (Math.pow(10, 9)))
                                            .availableInBytes((long) (Math.pow(10, 9)) / 2)
                                            .freeInBytes((long) (Math.pow(10, 9)) / 2)
                                            .build())
                                    .build())
                            .build());
                });

        NodeFSStatsResponse nodeFSStatsResponse = NodeFSStatsResponse.builder()
                .nodes(nodeFSDetailsMap)
                .build();

        Response response = Mockito.mock(Response.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
        InputStream inputStream = IOUtils.toInputStream(JsonUtils.toJson(nodeFSStatsResponse));
        Mockito.when(httpEntity.getContent())
                .thenReturn(inputStream);
        Mockito.when(response.getEntity())
                .thenReturn(httpEntity);

        Request nodeFSStatsRequest = new Request("GET", "_nodes/stats/fs?format=JSON");

        Mockito.when(mockLowLevelClient.performRequest(nodeFSStatsRequest))
                .thenReturn(response);
    }

    private void setupListIndicesMock() throws IOException {
        List<IndexInfoResponse> indexInfoResponses = INDICES.stream()
                .map(index -> IndexInfoResponse.builder()
                        .index(index)
                        .build())
                .collect(Collectors.toList());
        Response response = Mockito.mock(Response.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
        InputStream inputStream = IOUtils.toInputStream(JsonUtils.toJson(indexInfoResponses));
        Mockito.when(httpEntity.getContent())
                .thenReturn(inputStream);
        Mockito.when(response.getEntity())
                .thenReturn(httpEntity);

        Request listIndicesRequest = new Request("GET",
                String.format("_cat/indices/%s?format=JSON&bytes=b", ElasticsearchUtils.getTableIndexPattern()));
        Mockito.when(mockLowLevelClient.performRequest(listIndicesRequest))
                .thenReturn(response);
    }

}
