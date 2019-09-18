package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Slf4j
public class ClusterRerouteManager {

    private final ElasticsearchConnection connection;
    private final ClusterRerouteConfig clusterRerouteConfig;

    private Map<String, NodeInfo> nodeIdVsNodeInfoMap;
    private BiMap<String, String> nodeNameVsNodeId;

    public ClusterRerouteManager(ElasticsearchConnection connection, ClusterRerouteConfig clusterRerouteConfig) {
        this.connection = connection;
        this.clusterRerouteConfig = clusterRerouteConfig;
        this.nodeIdVsNodeInfoMap = new HashMap<>();
        this.nodeNameVsNodeId = HashBiMap.create();
    }

    public void reallocate() {
        this.createNodeInfoMap();
        this.createNodeNameVsNodeIdMap();

        int totalShards = getTotalShardCount();
        double avgShardsPerNode = Math.ceil((double) totalShards / (double) nodeIdVsNodeInfoMap.size());
        double acceptableShardsPerNode = avgShardsPerNode +
                Math.ceil((avgShardsPerNode * clusterRerouteConfig.getThresholdShardCountPercentage()) / 100);
        Deque<String> vacantNodeIds = getVacantNodeId((int) avgShardsPerNode);

        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo: nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue().getShardInfos().size();
            if (shardCount > acceptableShardsPerNode) {
                for (int i = shardCount; i > (int) avgShardsPerNode; i--) {
                    ShardId shardId = nodeIdVsNodeInfo.getValue().getShardInfos().get(i - 1).getShardId();
                    if (!vacantNodeIds.isEmpty()) {
                        reallocateShard(shardId, nodeIdVsNodeInfo.getKey(), vacantNodeIds.pop());
                    }
                }
            }
        }
    }

    private boolean reallocateShard(ShardId shardId, String fromNode, String toNode) {
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(shardId.getIndexName(), shardId.getId(), fromNode, toNode);
        ClusterRerouteRequest clusterRerouteRequest = new ClusterRerouteRequest();
        clusterRerouteRequest.add(moveAllocationCommand);
        try {
            ClusterRerouteResponse clusterRerouteResponse = connection.getClient()
                    .admin()
                    .cluster()
                    .reroute(clusterRerouteRequest)
                    .actionGet();
            log.info(String.format("Reallocating Shard. From Node: %s To Node: %s", fromNode, toNode));
            Thread.sleep(30000);
            return clusterRerouteResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(String.format("Error in Reallocating Shard. From Node: %s To Node: %s. Error Message: %s", fromNode, toNode, e.getMessage()), e);
            return false;
        }
    }

    private void createNodeInfoMap() {
        nodeIdVsNodeInfoMap.clear();
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = connection.getClient()
                .admin()
                .indices()
                .stats(indicesStatsRequest)
                .actionGet();
        Arrays.stream(indicesStatsResponse.getShards())
                .forEach(shardStats -> {
                    if(shardStats.getShardRouting()
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
                        if(nodeIdVsNodeInfoMap.containsKey(nodeId)) {
                            nodeIdVsNodeInfoMap.get(nodeId)
                                    .getShardInfos()
                                    .add(shardInfo);
                        } else {
                            List<ShardInfo> shardInfoList = Lists.newArrayList(shardInfo);
                            NodeInfo nodeInfo = NodeInfo.builder()
                                    .shardInfos(shardInfoList)
                                    .build();
                            nodeIdVsNodeInfoMap.put(nodeId, nodeInfo);
                        }
                    }
                });
    }

    private void createNodeNameVsNodeIdMap() {
        nodeNameVsNodeId.clear();
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.all();
        NodesInfoResponse nodesInfoResponse = connection.getClient()
                .admin()
                .cluster()
                .nodesInfo(nodesInfoRequest)
                .actionGet();
        nodesInfoResponse.getNodes()
                .forEach(nodeInfo -> nodeNameVsNodeId.put(nodeInfo.getNode()
                        .getName(), nodeInfo.getNode()
                        .getId()));
    }

    private int getTotalShardCount() {
        int totalShards = 0;
        for (NodeInfo nodeInfo : nodeIdVsNodeInfoMap.values()) {
            totalShards += nodeInfo.getShardInfos().size();
        }
        return totalShards;
    }

    private Deque<String> getVacantNodeId(int avgShardsPerNode) {
        Deque<String> vacantNodeIds = new ArrayDeque<>();
        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo: nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue().getShardInfos().size();
            if (shardCount < avgShardsPerNode) {
                for (int i = avgShardsPerNode; i > shardCount; i--) {
                    vacantNodeIds.push(nodeIdVsNodeInfo.getKey());
                }
            }
        }
        return vacantNodeIds;
    }

}
