package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.index.shard.ShardId;
import org.joda.time.DateTime;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Slf4j
@Singleton
public class ClusterRerouteManager {

    private final ElasticsearchConnection connection;
    private final ClusterRerouteConfig clusterRerouteConfig;

    private static final String CLUSTER_REROUTE_ENDPOINT = "/_cluster/reroute";
    private static final String INDICES_STATS_ENDPOINT = "/_stats/_all";
    private static final String NODES_INFO_ENDPOINT = "/_nodes";

    private static final String COMMANDS = "commands";
    private static final String MOVE_COMMAND = "move";
    private static final String INDEX_NAME = "index";
    private static final String SHARD_ID = "shard";
    private static final String FROM_NODE = "from_node";
    private static final String TO_NODE = "to_node";

    @Inject
    public ClusterRerouteManager(ElasticsearchConnection connection,
                                 ClusterRerouteConfig clusterRerouteConfig) {
        this.connection = connection;
        this.clusterRerouteConfig = clusterRerouteConfig;
    }

    public void reallocate() throws IOException {
        Map<String, NodeInfo> nodeIdVsNodeInfoMap = new HashMap<>();
        BiMap<String, String> nodeNameVsNodeId = HashBiMap.create();
        this.createNodeInfoMap(nodeIdVsNodeInfoMap);
        this.createNodeNameVsNodeIdMap(nodeNameVsNodeId);

        int totalShards = getTotalShardCount(nodeIdVsNodeInfoMap);
        double avgShardsPerNode = Math.ceil((double) totalShards / (double) nodeIdVsNodeInfoMap.size());
        double acceptableShardsPerNode = avgShardsPerNode + Math.ceil(
                (avgShardsPerNode * clusterRerouteConfig.getThresholdShardCountPercentage()) / 100);
        Deque<String> vacantNodeIds = getVacantNodeId((int) avgShardsPerNode, nodeIdVsNodeInfoMap);

        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo : nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue()
                    .getShardInfos()
                    .size();
            if (shardCount > acceptableShardsPerNode) {
                for (int i = shardCount; i >= (int) avgShardsPerNode; i--) {
                    ShardId shardId = nodeIdVsNodeInfo.getValue()
                            .getShardInfos()
                            .get(i - 1)
                            .getShardId();
                    if (!vacantNodeIds.isEmpty()) {
                        reallocateShard(shardId, nodeIdVsNodeInfo.getKey(), vacantNodeIds.pop());
                    }
                }
            }
        }
    }

    private boolean reallocateShard(ShardId shardId,
                                    String fromNode,
                                    String toNode) {

        Request clusterRerouteRequest = new Request("POST", CLUSTER_REROUTE_ENDPOINT);

        Map<String, Object> jsonMap = getShardMoveCommand(shardId, fromNode, toNode);

        clusterRerouteRequest.setJsonEntity(JsonUtils.toJson(jsonMap));

        try {
            ClusterRerouteResponse clusterRerouteResponse  =
                    JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(clusterRerouteRequest)
                    .getEntity()
                    .getContent()), ClusterRerouteResponse.class);
            log.info(String.format("Reallocating Shard. From Node: %s To Node: %s", fromNode, toNode));
            Thread.sleep((new Date(DateTime.now()).getHourOfDay() + 1) * 4000L);
            return clusterRerouteResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(
                    String.format("Error in Reallocating Shard. From Node: %s To Node: %s. Error Message: %s", fromNode,
                            toNode, e.getMessage()), e);
            return false;
        }
    }

    private Map<String, Object> getShardMoveCommand(ShardId shardId,
                                                    String fromNode,
                                                    String toNode) {
        Map<String, Object> moveCommandSpecs = new HashMap<>();
        moveCommandSpecs.put(INDEX_NAME, shardId.getIndexName());
        moveCommandSpecs.put(SHARD_ID, shardId.getId());
        moveCommandSpecs.put(FROM_NODE, fromNode);
        moveCommandSpecs.put(TO_NODE, toNode);

        Map<String, Object> moveCommand = new HashMap<>();
        moveCommand.put(MOVE_COMMAND, moveCommandSpecs);

        List<Map<String, Object>> list = new ArrayList<>();
        list.add(moveCommand);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put(COMMANDS, list);
        return jsonMap;
    }

    private void createNodeInfoMap(Map<String, NodeInfo> nodeIdVsNodeInfoMap) throws IOException {
        nodeIdVsNodeInfoMap.clear();
        Request indicesStatsRequest = new Request("GET", INDICES_STATS_ENDPOINT);

        try {
            IndicesStatsResponse indicesStatsResponse  =
                    JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                            .getLowLevelClient()
                            .performRequest(indicesStatsRequest)
                            .getEntity()
                            .getContent()), IndicesStatsResponse.class);
            Arrays.stream(indicesStatsResponse.getShards())
                    .forEach(shardStats -> {
                        if (shardStats.getShardRouting()
                                .shardId()
                                .getIndexName()
                                .matches(ElasticsearchUtils.getTodayIndicesPattern()) && !shardStats.getShardRouting()
                                .relocating()) {
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
                            } else {
                                List<ShardInfo> shardInfoList = Lists.newArrayList(shardInfo);
                                NodeInfo nodeInfo = NodeInfo.builder()
                                        .shardInfos(shardInfoList)
                                        .build();
                                nodeIdVsNodeInfoMap.put(nodeId, nodeInfo);
                            }
                        }
                    });
        } catch (IOException e) {
            log.error("Failed to create node info map , error : ", e);
            throw e;
        }
    }

    private void createNodeNameVsNodeIdMap(BiMap<String, String> nodeNameVsNodeId) throws IOException {
        nodeNameVsNodeId.clear();
        Request nodesInfoRequest = new Request("GET", NODES_INFO_ENDPOINT);
        try {
            NodesInfoResponse nodesInfoResponse = JsonUtils.fromJson(
                    IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(nodesInfoRequest)
                    .getEntity()
                    .getContent()), NodesInfoResponse.class);
            nodesInfoResponse.getNodes()
                    .forEach(nodeInfo -> nodeNameVsNodeId.put(nodeInfo.getNode()
                            .getName(), nodeInfo.getNode()
                            .getId()));
        } catch (IOException e){
            log.error("Failed to create name node vs node id map , error : ", e);
            throw e;
        }

    }

    private int getTotalShardCount(Map<String, NodeInfo> nodeIdVsNodeInfoMap) {
        int totalShards = 0;
        for (NodeInfo nodeInfo : nodeIdVsNodeInfoMap.values()) {
            totalShards += nodeInfo.getShardInfos()
                    .size();
        }
        return totalShards;
    }

    private Deque<String> getVacantNodeId(int avgShardsPerNode,
                                          Map<String, NodeInfo> nodeIdVsNodeInfoMap) {
        Deque<String> vacantNodeIds = new ArrayDeque<>();
        for (Map.Entry<String, NodeInfo> nodeIdVsNodeInfo : nodeIdVsNodeInfoMap.entrySet()) {
            int shardCount = nodeIdVsNodeInfo.getValue()
                    .getShardInfos()
                    .size();
            if (shardCount < avgShardsPerNode) {
                for (int i = avgShardsPerNode; i > shardCount; i--) {
                    vacantNodeIds.push(nodeIdVsNodeInfo.getKey());
                }
            }
        }
        return vacantNodeIds;
    }

}
