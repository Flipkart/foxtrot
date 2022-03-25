package com.flipkart.foxtrot.core.rebalance;

import com.flipkart.foxtrot.common.elasticsearch.shard.ShardMovementRequest;
import com.flipkart.foxtrot.common.elasticsearch.shard.ShardMovementRequest.MoveCommand;
import com.flipkart.foxtrot.common.elasticsearch.shard.ShardMovementRequest.MoveOperation;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.config.ShardRebalanceJobConfig;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.events.model.shardrebalance.*;
import com.flipkart.foxtrot.core.events.model.shardrebalance.ShardRebalanceSkipEvent.ShardRebalanceSkipReason;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.Request;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.nodeRegexPattern;
import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.FORMATTER;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Slf4j
@Singleton
public class ClusterRebalanceService {

    private static final String CLUSTER_REROUTE_ENDPOINT = "/_cluster/reroute";
    private static final String RELOCATING_STATE = "RELOCATING";
    private final ElasticsearchConnection connection;
    private final ShardRebalanceJobConfig shardRebalanceJobConfig;
    private final NodeGroupManager nodeGroupManager;
    private final EventBusManager eventBusManager;
    private final QueryStore queryStore;

    @Inject
    public ClusterRebalanceService(final ElasticsearchConnection connection,
                                   final ShardRebalanceJobConfig shardRebalanceJobConfig,
                                   final NodeGroupManager nodeGroupManager,
                                   final EventBusManager eventBusManager,
                                   final QueryStore queryStore) {
        this.connection = connection;
        this.shardRebalanceJobConfig = shardRebalanceJobConfig;
        this.nodeGroupManager = nodeGroupManager;
        this.eventBusManager = eventBusManager;
        this.queryStore = queryStore;
    }

    public void rebalanceShards() {
        rebalanceShards(FORMATTER.print(LocalDate.now()));
    }

    public void rebalanceShards(String datePostFix) {
        try {
            List<AllocatedESNodeGroup> allocatedNodeGroups = nodeGroupManager.getNodeGroups()
                    .stream()
                    .filter(esNodeGroup -> esNodeGroup.getStatus()
                            .equals(AllocationStatus.ALLOCATED))
                    .map(AllocatedESNodeGroup.class::cast)
                    .collect(Collectors.toList());
            Map<String, NodeInfo> nodeNameVsNodeInfoMap = createNodeInfoMap(datePostFix);
            allocatedNodeGroups.forEach(nodeGroup -> {
                List<Pattern> nodeRegexPatterns = nodeGroup.getNodePatterns()
                        .stream()
                        .map(nodeRegexPattern())
                        .collect(Collectors.toList());

                Map<String, NodeInfo> nodeNameVsNodeInfoForGroup = nodeNameVsNodeInfoMap.entrySet()
                        .stream()
                        .filter(entry -> nodeRegexPatterns.stream()
                                .anyMatch(pattern -> pattern.matcher(entry.getKey())
                                        .matches()))
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                rebalanceShards(nodeGroup.getGroupName(), datePostFix, nodeNameVsNodeInfoForGroup);
            });
            eventBusManager.postEvent(buildShardRebalanceJobFinishEvent(datePostFix).toIngestionEvent());
        } catch (Exception e) {
            log.error("Error while rebalancing shards : ", e);
            eventBusManager.postEvent(buildShardRebalanceJobFailureEvent(datePostFix, e).toIngestionEvent());
        }

    }

    private void rebalanceShards(String nodeGroup,
                                 String datePostFix,
                                 Map<String, NodeInfo> nodeNameVsNodeInfoForGroup) {
        log.info("Rebalancing shards in node group : {}", nodeGroup);
        log.debug("Rebalancing shards in node group : {}, nodeNameVsNodeInfoForGroup :{}", nodeGroup,
                nodeNameVsNodeInfoForGroup);

        ShardRebalanceContext shardRebalanceContext = buildShardRebalanceContext(nodeGroup, datePostFix,
                nodeNameVsNodeInfoForGroup, shardRebalanceJobConfig);

        eventBusManager.postEvent(buildShardCountStatusCheckEvent(shardRebalanceContext).toIngestionEvent());

        log.info("Rebalancing shards in node group : {}, Vacant Nodes: {}", nodeGroup,
                shardRebalanceContext.getVacantNodeNames());

        for (Entry<String, NodeInfo> nodeNameVsNodeInfo : nodeNameVsNodeInfoForGroup.entrySet()) {
            int shardCount = nodeNameVsNodeInfo.getValue()
                    .getShardInfos()
                    .size();
            if (shardCount > 0 && shardCount > shardRebalanceContext.getAvgShardsPerNode()) {
                balanceShardCountOnNode(shardRebalanceContext, nodeNameVsNodeInfo.getKey(),
                        nodeNameVsNodeInfo.getValue(), shardCount);
            } else {
                eventBusManager.postEvent(
                        buildShardRebalanceSkipEvent(shardRebalanceContext, nodeNameVsNodeInfo.getKey(), shardCount,
                                ShardRebalanceSkipReason.ALREADY_BALANCED).toIngestionEvent());
                log.info(
                        "Shard Count already balanced on node name : {} in node group : {} for date: {}. Skip Rebalancing shards ",
                        nodeNameVsNodeInfo.getKey(), nodeGroup, datePostFix);
            }
        }

    }

    private ShardRebalanceContext buildShardRebalanceContext(String nodeGroup,
                                                             String datePostFix,
                                                             Map<String, NodeInfo> nodeNameVsNodeInfoForGroup,
                                                             ShardRebalanceJobConfig rebalanceJobConfig) {
        Map<String, Integer> shardCountPerNode = getShardCountPerNode(nodeNameVsNodeInfoForGroup);
        int totalShards = getTotalShardCount(shardCountPerNode);
        int avgShardsPerNode = (int) Math.ceil((double) totalShards / (double) nodeNameVsNodeInfoForGroup.size());
        int maxAcceptableShardsPerNode = (int) (avgShardsPerNode + Math.ceil(
                (avgShardsPerNode * shardRebalanceJobConfig.getShardCountThresholdPercentage()) / 100));

        log.info(
                "Rebalancing shards in node group : {}, Total Shards: {}, Average Shards per Node: {}, Shard Count Threshold Percentage: {},"
                        + " Acceptable Shards Per Node: {}", nodeGroup, totalShards, avgShardsPerNode,
                shardRebalanceJobConfig.getShardCountThresholdPercentage(), maxAcceptableShardsPerNode);
        Deque<String> vacantNodeNames = getVacantNodeNames(avgShardsPerNode, maxAcceptableShardsPerNode,
                nodeNameVsNodeInfoForGroup);
        return ShardRebalanceContext.builder()
                .nodeGroup(nodeGroup)
                .datePostFix(datePostFix)
                .avgShardsPerNode(avgShardsPerNode)
                .totalShards(totalShards)
                .maxAcceptableShardsPerNode(maxAcceptableShardsPerNode)
                .nodeNameVsNodeInfo(nodeNameVsNodeInfoForGroup)
                .shardCountPerNode(shardCountPerNode)
                .vacantNodeNames(vacantNodeNames)
                .shardCountThresholdPercentage(rebalanceJobConfig.getShardCountThresholdPercentage())
                .build();
    }

    private void balanceShardCountOnNode(ShardRebalanceContext shardRebalanceContext,
                                         String nodeName,
                                         NodeInfo nodeInfo,
                                         int shardCount) {
        int initialShardCount = shardCount;
        while (shardCount > shardRebalanceContext.getAvgShardsPerNode()) {
            ShardInfo shardInfo = nodeInfo.getShardInfos()
                    .get(shardCount - 1);
            if (!shardRebalanceContext.getVacantNodeNames()
                    .isEmpty()) {
                reallocateShard(shardRebalanceContext, shardInfo, nodeName, shardRebalanceContext.getVacantNodeNames()
                        .poll());
            } else {
                log.info(
                        "Rebalancing shards in node group :{}, Could not rellocate shard :{} for index :{} because vacant node names are exhausted",
                        shardRebalanceContext.getNodeGroup(), shardInfo.getShard(), shardInfo.getIndex());
                eventBusManager.postEvent(buildShardRebalanceSkipEvent(shardRebalanceContext, nodeName, shardCount,
                        ShardRebalanceSkipReason.NO_VACANT_NODES).toIngestionEvent());
            }
            shardCount--;
        }
        eventBusManager.postEvent(buildShardRebalanceFinishEvent(shardRebalanceContext.getNodeGroup(),
                shardRebalanceContext.getDatePostFix(), nodeName, initialShardCount, shardCount).toIngestionEvent());
        log.info("Successfully finished Rebalancing shards for node name : {} in node group : {} for date: {}",
                nodeName, shardRebalanceContext.getNodeGroup(), shardRebalanceContext.getDatePostFix());
    }

    private Map<String, Integer> getShardCountPerNode(Map<String, NodeInfo> nodeNameVsNodeInfoMap) {
        Map<String, Integer> shardCountPerNode = new HashMap<>();
        nodeNameVsNodeInfoMap.forEach((nodeName, nodeInfo) -> shardCountPerNode.put(nodeName,
                shardCountPerNode.getOrDefault(nodeName, 0) + nodeInfo.getShardInfos()
                        .size()));
        return shardCountPerNode;
    }

    protected boolean reallocateShard(ShardRebalanceContext shardRebalanceContext,
                                      ShardInfo shardInfo,
                                      String fromNode,
                                      String toNode) {
        ShardMovementRequest shardMovementRequest = buildShardMovementRequest(shardInfo, fromNode, toNode);
        String moveCommand = JsonUtils.toJson(shardMovementRequest);
        Request clusterRerouteRequest = new Request("POST", CLUSTER_REROUTE_ENDPOINT);
        clusterRerouteRequest.setJsonEntity(moveCommand);

        log.info("Rebalancing shards in node group : {}, Shard Move Command Request: {}",
                shardRebalanceContext.getNodeGroup(), moveCommand);

        try {
            ClusterRerouteResponseInfo clusterRerouteResponse = JsonUtils.fromJson(IOUtils.toByteArray(
                    connection.getClient()
                            .getLowLevelClient()
                            .performRequest(clusterRerouteRequest)
                            .getEntity()
                            .getContent()), ClusterRerouteResponseInfo.class);
            log.info(
                    "Rebalancing shards in node group : {} , Shard Reallocation Acknowledged Status: {}. From Node: {} To Node: {}, Index: {} , Shard Id: {}",
                    shardRebalanceContext.getNodeGroup(), clusterRerouteResponse.isAcknowledged(), fromNode, toNode,
                    shardInfo.getIndex(), shardInfo.getShard());
            eventBusManager.postEvent(buildShardMovementStatusEvent(shardRebalanceContext.getNodeGroup(),
                    shardRebalanceContext.getDatePostFix(), moveCommand, fromNode, toNode,
                    clusterRerouteResponse.isAcknowledged()).toIngestionEvent());
            return clusterRerouteResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(
                    "Error in Rebalancing shards in node group : {} Reallocating shard. From Node: {} To Node: {}. Index :{}, Shard Id:{}, Error :",
                    fromNode, toNode, shardInfo.getIndex(), shardInfo.getShard(), e);
            eventBusManager.postEvent(buildShardMovementFailureEvent(shardRebalanceContext.getNodeGroup(),
                    shardRebalanceContext.getDatePostFix(), moveCommand, fromNode, toNode, e).toIngestionEvent());
            return false;
        }
    }


    private ShardMovementRequest buildShardMovementRequest(ShardInfo shardInfo,
                                                           String fromNode,
                                                           String toNode) {

        return ShardMovementRequest.builder()
                .commands(Collections.singletonList(MoveCommand.builder()
                        .move(MoveOperation.builder()
                                .fromNode(fromNode)
                                .toNode(toNode)
                                .index(shardInfo.getIndex())
                                .shard(shardInfo.getShard())
                                .build())
                        .build()))
                .build();
    }

    // populate a map with key = node (prd-esxx.xx.nmx)
    // and value = [{"shard": "0", "index":"foxtrot-groupon-table-06-4-2021"}]
    private Map<String, NodeInfo> createNodeInfoMap(String datePostFix) throws IOException {
        Map<String, NodeInfo> nodeNameVsNodeInfoMap = new HashMap<>();

        try {
            List<ShardInfoResponse> shardInfoResponses = queryStore.getTableShardsInfo();

            shardInfoResponses.forEach(shardResponse -> {
                if (shardResponse.getIndex()
                        .matches(ElasticsearchUtils.getIndicesPatternForDate(datePostFix)) && !RELOCATING_STATE.equals(
                        shardResponse.getState())) {
                    ShardInfo shardInfo = ShardInfo.builder()
                            .shard(shardResponse.getShard())
                            .index(shardResponse.getIndex())
                            .build();
                    String nodeName = shardResponse.getNode();
                    if (!Strings.isNullOrEmpty(nodeName)) {
                        if (nodeNameVsNodeInfoMap.containsKey(nodeName)) {
                            nodeNameVsNodeInfoMap.get(nodeName)
                                    .getShardInfos()
                                    .add(shardInfo);
                        } else {
                            List<ShardInfo> shardInfoList = Lists.newArrayList(shardInfo);
                            NodeInfo nodeInfo = NodeInfo.builder()
                                    .shardInfos(shardInfoList)
                                    .build();
                            nodeNameVsNodeInfoMap.put(nodeName, nodeInfo);
                        }
                    }
                }
            });
            log.info("Created Node Info Map : {}", nodeNameVsNodeInfoMap);
            return nodeNameVsNodeInfoMap;
        } catch (Exception e) {
            log.error("Failed to create node info map , error : ", e);
            throw e;
        }
    }

    private int getTotalShardCount(Map<String, Integer> shardCountPerNode) {
        return shardCountPerNode.values()
                .stream()
                .mapToInt(i -> i)
                .sum();
    }

    /*
        Will return a queue with nodes which can store more shards
     */
    private Deque<String> getVacantNodeNames(int avgShardsPerNode,
                                             int maxAcceptableShardsPerNode,
                                             Map<String, NodeInfo> nodeNameVsNodeInfoMap) {
        Deque<String> vacantNodeNames = new ArrayDeque<>();

        // logic to host at least avgShardsPerNode on each node
        for (Map.Entry<String, NodeInfo> nodeNameVsNodeInfo : nodeNameVsNodeInfoMap.entrySet()) {
            int shardCount = nodeNameVsNodeInfo.getValue()
                    .getShardInfos()
                    .size();
            while (shardCount < avgShardsPerNode) {
                vacantNodeNames.push(nodeNameVsNodeInfo.getKey());
                shardCount++;
            }
        }

        // logic to host maxAcceptableShardsPerNode on each node after it has hosted avgShardsPerNode
        for (Map.Entry<String, NodeInfo> nodeNameVsNodeInfo : nodeNameVsNodeInfoMap.entrySet()) {
            int shardCount = avgShardsPerNode;
            while (shardCount < maxAcceptableShardsPerNode) {
                vacantNodeNames.push(nodeNameVsNodeInfo.getKey());
                shardCount++;
            }
        }
        return vacantNodeNames;
    }

    private ShardCountStatusCheckEvent buildShardCountStatusCheckEvent(ShardRebalanceContext shardRebalanceContext) {
        return ShardCountStatusCheckEvent.builder()
                .nodeGroup(shardRebalanceContext.getNodeGroup())
                .datePostFix(shardRebalanceContext.getDatePostFix())
                .shardCountPerNode(JsonUtils.toJson(shardRebalanceContext.getShardCountPerNode()))
                .totalShards(shardRebalanceContext.getTotalShards())
                .avgShardsPerNode(shardRebalanceContext.getAvgShardsPerNode())
                .maxAcceptableShardsPerNode(shardRebalanceContext.getMaxAcceptableShardsPerNode())
                .shardCountThresholdPercentage(shardRebalanceContext.getShardCountThresholdPercentage())
                .build();
    }

    protected ShardMovementFailureEvent buildShardMovementFailureEvent(String nodeGroup,
                                                                       String datePostFix,
                                                                       String moveCommand,
                                                                       String fromNode,
                                                                       String toNode,
                                                                       Exception exception) {
        return ShardMovementFailureEvent.builder()
                .nodeGroup(nodeGroup)
                .datePostFix(datePostFix)
                .moveCommand(moveCommand)
                .fromNode(fromNode)
                .toNode(toNode)
                .exception(exception.toString())
                .exceptionMessage(exception.getMessage())
                .exceptionCause(exception.getCause() == null
                        ? "null"
                        : exception.getCause()
                        .toString())
                .build();
    }

    private ShardMovementStatusEvent buildShardMovementStatusEvent(String nodeGroup,
                                                                   String datePostFix,
                                                                   String moveCommand,
                                                                   String fromNode,
                                                                   String toNode,
                                                                   boolean acknowledged) {
        return ShardMovementStatusEvent.builder()
                .nodeGroup(nodeGroup)
                .datePostFix(datePostFix)
                .moveCommand(moveCommand)
                .fromNode(fromNode)
                .toNode(toNode)
                .acknowledged(acknowledged)
                .build();
    }

    private ShardRebalanceSkipEvent buildShardRebalanceSkipEvent(ShardRebalanceContext shardRebalanceContext,
                                                                 String nodeName,
                                                                 int shardCount,
                                                                 ShardRebalanceSkipReason skipReason) {
        return ShardRebalanceSkipEvent.builder()
                .nodeGroup(shardRebalanceContext.getNodeGroup())
                .nodeName(nodeName)
                .shardCount(shardCount)
                .datePostFix(shardRebalanceContext.getDatePostFix())
                .avgShardsPerNode(shardRebalanceContext.getAvgShardsPerNode())
                .maxAcceptableShardsPerNode(shardRebalanceContext.getMaxAcceptableShardsPerNode())
                .skipReason(skipReason)
                .build();
    }

    private ShardRebalanceFinishEvent buildShardRebalanceFinishEvent(String nodeGroup,
                                                                     String datePostFix,
                                                                     String nodeName,
                                                                     int initialShardCount,
                                                                     int finalShardCount) {
        return ShardRebalanceFinishEvent.builder()
                .nodeGroup(nodeGroup)
                .datePostFix(datePostFix)
                .nodeName(nodeName)
                .initialShardCount(initialShardCount)
                .finalShardCount(finalShardCount)
                .build();
    }

    protected ShardRebalanceJobFailureEvent buildShardRebalanceJobFailureEvent(String datePostFix,
                                                                               Exception e) {
        return ShardRebalanceJobFailureEvent.builder()
                .datePostFix(datePostFix)
                .exception(e.toString())
                .exceptionCause(e.getCause() == null
                        ? "null"
                        : e.getCause()
                        .toString())
                .exceptionMessage(e.getMessage())
                .build();
    }

    private ShardRebalanceJobFinishEvent buildShardRebalanceJobFinishEvent(String datePostFix) {
        return ShardRebalanceJobFinishEvent.builder()
                .datePostFix(datePostFix)
                .build();
    }
}
