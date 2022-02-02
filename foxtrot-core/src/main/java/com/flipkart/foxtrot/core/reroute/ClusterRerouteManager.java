package com.flipkart.foxtrot.core.reroute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.shard.ShardId;
import org.joda.time.DateTime;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Slf4j
@Singleton
@Order(20)
public class ClusterRerouteManager {

    private final ElasticsearchConnection connection;
    private final ClusterRerouteConfig clusterRerouteConfig;
    private final ObjectMapper mapper;

    @Inject
    public ClusterRerouteManager(
            ElasticsearchConnection connection,
            ClusterRerouteConfig clusterRerouteConfig,
            ObjectMapper mapper) {
        this.connection = connection;
        this.clusterRerouteConfig = clusterRerouteConfig;
        this.mapper = mapper;
    }

    //TODO This needs fixing to correspond to changed response from java high level client

    /*public void reallocate() {
        Map<String, NodeInfo> nodeIdVsNodeInfoMap = new HashMap<>();
        BiMap<String, String> nodeNameVsNodeId = HashBiMap.create();
        this.createNodeInfoMap(nodeIdVsNodeInfoMap);
        this.createNodeNameVsNodeIdMap(nodeNameVsNodeId);

        int totalShards = getTotalShardCount(nodeIdVsNodeInfoMap);
        double avgShardsPerNode = Math.ceil((double) totalShards / (double) nodeIdVsNodeInfoMap.size());
        double acceptableShardsPerNode = avgShardsPerNode +
                Math.ceil((avgShardsPerNode * clusterRerouteConfig.getThresholdShardCountPercentage()) / 100);
        Deque<String> vacantNodeIds = getVacantNodeId((int) avgShardsPerNode, nodeIdVsNodeInfoMap);

        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo : nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue().getShardInfos().size();
            if (shardCount > acceptableShardsPerNode) {
                for (int i = shardCount; i >= (int) avgShardsPerNode; i--) {
                    ShardId shardId = nodeIdVsNodeInfo.getValue().getShardInfos().get(i - 1).getShardId();
                    if (!vacantNodeIds.isEmpty()) {
                        reallocateShard(shardId, nodeIdVsNodeInfo.getKey(), vacantNodeIds.pop());
                    }
                }
            }
        }
    }

    private boolean reallocateShard(ShardId shardId, String fromNode, String toNode) {
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(shardId.getIndexName(),
                                                                                shardId.getId(),
                                                                                fromNode,
                                                                                toNode);
        val clusterRerouteRequest = new ClusterRerouteRequest();
        clusterRerouteRequest.add(moveAllocationCommand);
        try {
            val request = new Request("POST", "/cluster/reroute");
            request.setEntity(new StringEntity(mapper.writeValueAsString(clusterRerouteRequest)));
            val response = connection.getClient()
                    .getLowLevelClient()
                    .performRequest(request);

            val clusterRerouteResponse = mapper.readValue(EntityUtils.toString(response.getEntity()),
                                                          ClusterRerouteResponse.class);
            log.info(String.format("Reallocating Shard. From Node: %s To Node: %s", fromNode, toNode));
            Thread.sleep((new Date(DateTime.now()).getHourOfDay() + 1) * 4000L);
            return clusterRerouteResponse.isAcknowledged();
        }
        catch (Exception e) {
            log.error(String.format("Error in Reallocating Shard. From Node: %s To Node: %s. Error Message: %s",
                                    fromNode,
                                    toNode,
                                    e.getMessage()), e);
            return false;
        }
    }

    @SneakyThrows
    private void createNodeInfoMap(Map<String, NodeInfo> nodeIdVsNodeInfoMap) {
        nodeIdVsNodeInfoMap.clear();
        val request = new Request("GET", "/_stats");
        Response response = connection.getClient()
                .getLowLevelClient()
                .performRequest(request);

        val indicesStatsResponse = mapper.readValue(EntityUtils.toString(response.getEntity()),
                                                    IndicesStatsResponse.class);
        Arrays.stream(indicesStatsResponse.getShards())
                .forEach(shardStats -> {
                    if (shardStats.getShardRouting()
                            .shardId()
                            .getIndexName()
                            .matches(ElasticsearchUtils.getTodayIndicesPattern())
                            && !shardStats.getShardRouting().relocating()) {
                        ShardId shardId = shardStats.getShardRouting()
                                .shardId();
                        ShardInfo shardInfo = ShardInfo.builder()
                                .shardId(shardId)
                                .build();
                        String nodeId = shardStats.getShardRouting()
                                .currentNodeId();
                        if (nodeIdVsNodeInfoMap.containsKey(nodeId)) {
                            nodeIdVsNodeInfoMap.get(nodeId)
                                    .getShardInfos()
                                    .add(shardInfo);
                        }
                        else {
                            List<ShardInfo> shardInfoList = Lists.newArrayList(shardInfo);
                            NodeInfo nodeInfo = NodeInfo.builder()
                                    .shardInfos(shardInfoList)
                                    .build();
                            nodeIdVsNodeInfoMap.put(nodeId, nodeInfo);
                        }
                    }
                });
    }

    @SneakyThrows
    private void createNodeNameVsNodeIdMap(
            BiMap<String, String> nodeNameVsNodeId) {
        nodeNameVsNodeId.clear();
        val request = new Request("GET", "/_stats");
        val response = connection.getClient()
                .getLowLevelClient()
                .performRequest(request);
        val nodesInfoResponse = mapper.readValue(EntityUtils.toString(response.getEntity()),
                                                    NodesInfoResponse.class);
        nodesInfoResponse.getNodes()
                .forEach(nodeInfo -> nodeNameVsNodeId.put(nodeInfo.getNode()
                                                                  .getName(), nodeInfo.getNode()
                                                                  .getId()));
    }

    private int getTotalShardCount(
            Map<String, NodeInfo> nodeIdVsNodeInfoMap) {
        int totalShards = 0;
        for (NodeInfo nodeInfo : nodeIdVsNodeInfoMap.values()) {
            totalShards += nodeInfo.getShardInfos().size();
        }
        return totalShards;
    }

    private Deque<String> getVacantNodeId(
            int avgShardsPerNode,
            Map<String, NodeInfo> nodeIdVsNodeInfoMap) {
        Deque<String> vacantNodeIds = new ArrayDeque<>();
        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo : nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue().getShardInfos().size();
            if (shardCount < avgShardsPerNode) {
                for (int i = avgShardsPerNode; i > shardCount; i--) {
                    vacantNodeIds.push(nodeIdVsNodeInfo.getKey());
                }
            }
        }
        return vacantNodeIds;
    }*/

}
