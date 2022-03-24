package com.flipkart.foxtrot.core.rebalance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.exception.NodeGroupStoreException;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.ShardRebalanceJobConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig.ConnectionType;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

@RunWith(MockitoJUnitRunner.class)
public class ClusterRebalanceServiceTest {

    private static final String CLUSTER_REROUTE_ENDPOINT = "/_cluster/reroute";
    private static final String SHARD_STATS_ENDPOINT = "/_cat/shards/%s?format=JSON&bytes=b";
    private static ElasticsearchConnection elasticsearchConnection;

    private static QueryStore queryStore;
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(9999, 9933);
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setup() throws Exception {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setConnectionType(ConnectionType.HTTP);
        elasticsearchConfig.setCluster("foxtrot");
        elasticsearchConfig.setHosts(Lists.newArrayList("localhost"));
        elasticsearchConfig.setPort(9999);

        elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);
        elasticsearchConnection.start();

        queryStore = new ElasticsearchQueryStore(Mockito.mock(TableMetadataManager.class),
                Mockito.mock(TenantMetadataManager.class), elasticsearchConnection, Mockito.mock(DataStore.class),
                new ArrayList<>(), Mockito.mock(ElasticsearchTemplateMappingParser.class), new CardinalityConfig());
        SerDe.init(new ObjectMapper());
    }

    @Before
    public void beforeMethod() {
        wireMockRule.resetMappings();
    }

    @Test
    public void shouldRebalanceShardCount() {

        ShardRebalanceJobConfig shardRebalanceJobConfig = new ShardRebalanceJobConfig();
        shardRebalanceJobConfig.setShardCountThresholdPercentage(30);

        NodeGroupManager nodeGroupManager = Mockito.mock(NodeGroupManager.class);
        EventBusManager eventBusManager = Mockito.mock(EventBusManager.class);

        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .tableAllocation(SpecificTableAllocation.builder()
                        .tables(Sets.newTreeSet(Sets.newHashSet("payment")))
                        .totalShardsPerNode(3)
                        .build())
                .groupName("payment")
                .nodePatterns(Sets.newTreeSet(Sets.newHashSet("elasticsearch1*", "elasticsearch2*", "elasticsearch3*")))
                .build();

        AllocatedESNodeGroup commonGroup = AllocatedESNodeGroup.builder()
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(3)
                        .build())
                .groupName("common")
                .nodePatterns(Sets.newTreeSet(Sets.newHashSet("elasticsearch4*", "elasticsearch5*", "elasticsearch6*")))
                .build();

        Mockito.when(nodeGroupManager.getNodeGroups())
                .thenReturn(Lists.newArrayList(paymentGroup, commonGroup));
        ClusterRebalanceService spyClusterRebalanceService = Mockito.spy(
                new ClusterRebalanceService(elasticsearchConnection, shardRebalanceJobConfig, nodeGroupManager,
                        eventBusManager, queryStore));

        List<ShardInfoResponse> shardInfoResponses = new ArrayList<>();

        shardInfoResponses.addAll(
                addShardsForIndex(0, "foxtrot-payment-table-04-10-2021", "elasticsearch101.test.nmx", 1));
        shardInfoResponses.addAll(
                addShardsForIndex(1, "foxtrot-payment-table-04-10-2021", "elasticsearch201.test.nmx", 2));
        shardInfoResponses.addAll(
                addShardsForIndex(3, "foxtrot-payment-table-04-10-2021", "elasticsearch301.test.nmx", 11));

        shardInfoResponses.addAll(
                addShardsForIndex(0, "foxtrot-nexus-table-04-10-2021", "elasticsearch401.test.nmx", 2));
        shardInfoResponses.addAll(
                addShardsForIndex(2, "foxtrot-nexus-table-04-10-2021", "elasticsearch501.test.nmx", 4));
        shardInfoResponses.addAll(
                addShardsForIndex(6, "foxtrot-nexus-table-04-10-2021", "elasticsearch601.test.nmx", 40));

        setupWireMockGet(String.format(SHARD_STATS_ENDPOINT, ElasticsearchUtils.getTableIndexPattern()), 200,
                JsonUtils.toJson(shardInfoResponses));
        setupWireMockPost(CLUSTER_REROUTE_ENDPOINT, 200, JsonUtils.toJson(ClusterRerouteResponseInfo.builder()
                .acknowledged(true)
                .build()));

        spyClusterRebalanceService.rebalanceShards("04-10-2021");

        Mockito.verify(spyClusterRebalanceService, Mockito.times(30))
                .reallocateShard(Mockito.any(ShardRebalanceContext.class), Mockito.any(ShardInfo.class),
                        Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void shouldFailShardMovementWhileRebalancing() {
        ShardRebalanceJobConfig shardRebalanceJobConfig = new ShardRebalanceJobConfig();
        shardRebalanceJobConfig.setShardCountThresholdPercentage(30);

        NodeGroupManager nodeGroupManager = Mockito.mock(NodeGroupManager.class);
        EventBusManager eventBusManager = Mockito.mock(EventBusManager.class);
        AllocatedESNodeGroup paymentGroup = AllocatedESNodeGroup.builder()
                .tableAllocation(SpecificTableAllocation.builder()
                        .tables(Sets.newTreeSet(Sets.newHashSet("payment")))
                        .totalShardsPerNode(3)
                        .build())
                .groupName("payment")
                .nodePatterns(Sets.newTreeSet(Sets.newHashSet("elasticsearch1*", "elasticsearch2*", "elasticsearch3*")))
                .build();

        AllocatedESNodeGroup commonGroup = AllocatedESNodeGroup.builder()
                .tableAllocation(CommonTableAllocation.builder()
                        .totalShardsPerNode(3)
                        .build())
                .groupName("common")
                .nodePatterns(Sets.newTreeSet(Sets.newHashSet("elasticsearch4*", "elasticsearch5*", "elasticsearch6*")))
                .build();

        Mockito.when(nodeGroupManager.getNodeGroups())
                .thenReturn(Lists.newArrayList(paymentGroup, commonGroup));
        ClusterRebalanceService spyClusterRebalanceService = Mockito.spy(
                new ClusterRebalanceService(elasticsearchConnection, shardRebalanceJobConfig, nodeGroupManager,
                        eventBusManager, queryStore));

        List<ShardInfoResponse> shardInfoResponses = new ArrayList<>();

        shardInfoResponses.addAll(
                addShardsForIndex(0, "foxtrot-payment-table-04-10-2021", "elasticsearch101.test.nmx", 1));
        shardInfoResponses.addAll(
                addShardsForIndex(1, "foxtrot-payment-table-04-10-2021", "elasticsearch201.test.nmx", 2));
        shardInfoResponses.addAll(
                addShardsForIndex(3, "foxtrot-payment-table-04-10-2021", "elasticsearch301.test.nmx", 11));

        shardInfoResponses.addAll(
                addShardsForIndex(0, "foxtrot-nexus-table-04-10-2021", "elasticsearch401.test.nmx", 2));
        shardInfoResponses.addAll(
                addShardsForIndex(2, "foxtrot-nexus-table-04-10-2021", "elasticsearch501.test.nmx", 4));
        shardInfoResponses.addAll(
                addShardsForIndex(6, "foxtrot-nexus-table-04-10-2021", "elasticsearch601.test.nmx", 40));

        setupWireMockGet(String.format(SHARD_STATS_ENDPOINT, ElasticsearchUtils.getTableIndexPattern()), 200,
                JsonUtils.toJson(shardInfoResponses));
        setupWireMockPost(CLUSTER_REROUTE_ENDPOINT, 500,
                JsonUtils.toJson(ImmutableMap.of("message", "something went wrong")));

        spyClusterRebalanceService.rebalanceShards("04-10-2021");

        Mockito.verify(spyClusterRebalanceService, Mockito.times(30))
                .buildShardMovementFailureEvent(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }

    @Test
    public void shouldFailShardRebalanceJob() {
        ShardRebalanceJobConfig shardRebalanceJobConfig = new ShardRebalanceJobConfig();
        shardRebalanceJobConfig.setShardCountThresholdPercentage(30);

        NodeGroupManager nodeGroupManager = Mockito.mock(NodeGroupManager.class);
        EventBusManager eventBusManager = Mockito.mock(EventBusManager.class);

        Mockito.when(nodeGroupManager.getNodeGroups())
                .thenThrow(new NodeGroupStoreException("Not found"));
        ClusterRebalanceService spyClusterRebalanceService = Mockito.spy(
                new ClusterRebalanceService(elasticsearchConnection, shardRebalanceJobConfig, nodeGroupManager,
                        eventBusManager, queryStore));
        spyClusterRebalanceService.rebalanceShards("04-10-2021");

        Mockito.verify(spyClusterRebalanceService, Mockito.times(1))
                .buildShardRebalanceJobFailureEvent(Mockito.anyString(), Mockito.any(Exception.class));
    }

    private List<ShardInfoResponse> addShardsForIndex(int startShardIndex,
                                                      String indexName,
                                                      String nodeName,
                                                      int noOfShards) {
        List<ShardInfoResponse> shardInfoResponses = Lists.newArrayList();
        for (int i = startShardIndex; i < startShardIndex + noOfShards; i++) {
            shardInfoResponses.add(ShardInfoResponse.builder()
                    .index(indexName)
                    .node(nodeName)
                    .shard(String.valueOf(i))
                    .state("STARTED")
                    .build());
        }

        return shardInfoResponses;
    }

    private void setupWireMockPost(String endpoint,
                                   int statusCode,
                                   String response) {
        WireMock wireMockClient = new WireMock("localhost", wireMockRule.port());
        wireMockClient.register(WireMock.post(urlEqualTo(endpoint))
                .willReturn(aResponse().withStatus(statusCode)
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withBody(response)));
    }

    private void setupWireMockGet(String endpoint,
                                  int statusCode,
                                  String response) {
        WireMock wireMockClient = new WireMock("localhost", wireMockRule.port());
        wireMockClient.register(WireMock.get(urlEqualTo(endpoint))
                .willReturn(aResponse().withStatus(statusCode)
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withBody(response)));
    }
}
