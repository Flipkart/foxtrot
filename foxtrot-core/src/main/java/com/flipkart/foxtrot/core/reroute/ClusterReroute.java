package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.core.alerts.EmailClient;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getTodayIndicesPattern;

/***
 Created by mudit.g on Feb, 2019
 ***/
public class ClusterReroute implements Runnable {

    private ElasticsearchConnection connection;
    private Map<String, String> nodeNameVsNodeId;
    Map<String, NodeInfo> nodeIdVsNodeInfoMap;
    private String fromNodeName;
    private EmailClient emailClient;
    private ClusterRerouteConfig clusterRerouteConfig;

    private static String Subject = "Shard Reallocation failed";

    public ClusterReroute(ElasticsearchConnection connection, String fromNodeName,
                          EmailClient emailClient, ClusterRerouteConfig clusterRerouteConfig) {
        this.connection = connection;
        this.fromNodeName = fromNodeName;
        this.emailClient = emailClient;
        this.clusterRerouteConfig = clusterRerouteConfig;
        this.nodeNameVsNodeId = new HashMap<>();
        this.nodeIdVsNodeInfoMap = new HashMap<>();
        createNodeInfoMap();
        createNodeNameVsNodeIdMap();
    }

    @Override
    public void run() {
        if (!nodeNameVsNodeId.containsKey(fromNodeName)) {
            emailClient.sendEmail(Subject, "Could not resolve fromNodeName: " + fromNodeName, clusterRerouteConfig.getRecipients());
            return;
        }
        String fromNodeId = nodeNameVsNodeId.get(fromNodeName);
        if (!nodeIdVsNodeInfoMap.containsKey(fromNodeId)) {
            emailClient.sendEmail(Subject, "Could not resolve fromNodeId: " + fromNodeId, clusterRerouteConfig.getRecipients());
            return;
        }
        LinkedList<Map.Entry<String, NodeInfo>> nodeInfoList = getSortedNodeInfoList();
        List<ShardInfo> shardInfos = nodeIdVsNodeInfoMap.get(fromNodeId).getShardInfos();
        shardInfos.sort(new Comparator<ShardInfo>() {
            @Override
            public int compare(ShardInfo o1, ShardInfo o2) {
                return Long.compare(o1.getShardSize(), o2.getShardSize());
            }
        });
        int noOfRetries = clusterRerouteConfig.getNoOfRetries();
        int retryNo;
        for (retryNo = 0; retryNo < noOfRetries; retryNo++) {
            String toNodeId = nodeInfoList.get(nodeInfoList.size() -1 -retryNo).getKey();
            boolean realocationSuccessfull = true;
            for (int i = 0; i < clusterRerouteConfig.getNoOfShardsToBeRealocated(); i++) {
                ShardId shardId = shardInfos.get(shardInfos.size() -1 -i).getShardId();
                if (!shardRealocation(shardId, fromNodeId, toNodeId)) {
                    realocationSuccessfull = false;
                }
            }
            if (realocationSuccessfull) {
                return;
            }
        }
        if (retryNo == noOfRetries) {
            emailClient.sendEmail(Subject, "Exceeded max no. of retries: " + nodeInfoList, clusterRerouteConfig.getRecipients());
        }
    }

    private boolean shardRealocation(ShardId shardId, String fromNode, String toNode) {
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(shardId, fromNode, toNode);
        ClusterRerouteRequest clusterRerouteRequest = new ClusterRerouteRequest();
        clusterRerouteRequest.add(moveAllocationCommand);
        try {
            ClusterRerouteResponse clusterRerouteResponse = connection.getClient()
                    .admin()
                    .cluster()
                    .reroute(clusterRerouteRequest)
                    .actionGet();
            return clusterRerouteResponse.isAcknowledged();
        } catch (Exception e) {
            return false;
        }
    }

    private void createNodeInfoMap() {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = connection.getClient().admin().indices().stats(indicesStatsRequest).actionGet();
        Arrays.stream(indicesStatsResponse.getShards()).forEach(
                shardStats -> {
                    if (shardStats.getShardRouting().shardId().getIndex().matches(getTodayIndicesPattern())) {
                        ShardId shardId = shardStats.getShardRouting().shardId();
                        long shardSize = shardStats.getStats().store.getSizeInBytes();
                        ShardInfo shardInfo = new ShardInfo(shardId, shardSize);
                        String nodeId = shardStats.getShardRouting().currentNodeId();
                        if (nodeIdVsNodeInfoMap.containsKey(nodeId)) {
                            long nodeSize = nodeIdVsNodeInfoMap.get(nodeId).getNodeSize() + shardSize;
                            nodeIdVsNodeInfoMap.get(nodeId).setNodeSize(nodeSize);
                            nodeIdVsNodeInfoMap.get(nodeId).getShardInfos().add(shardInfo);
                        } else {
                            List<ShardInfo> shardInfos = new ArrayList<>(Arrays.asList(shardInfo));
                            NodeInfo nodeInfo = new NodeInfo(shardSize, shardInfos);
                            nodeIdVsNodeInfoMap.put(nodeId, nodeInfo);
                        }
                    }
                }
        );
    }

    private LinkedList<Map.Entry<String, NodeInfo>> getSortedNodeInfoList() {
        LinkedList<Map.Entry<String, NodeInfo>> list = new LinkedList<>(nodeIdVsNodeInfoMap.entrySet());
        Comparator<Map.Entry<String, NodeInfo>> comparator = new Comparator<Map.Entry<String, NodeInfo>>() {
            @Override
            public int compare(Map.Entry<String, NodeInfo> o1, Map.Entry<String, NodeInfo> o2) {
                return Long.compare(o1.getValue().getNodeSize(), o2.getValue().getNodeSize());
            }
        };
        Collections.sort(list, comparator.reversed());
        return list;
    }

    private void createNodeNameVsNodeIdMap() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.all();
        NodesInfoResponse nodesInfoResponse = connection.getClient().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Arrays.stream(nodesInfoResponse.getNodes()).forEach(nodeInfo ->
                nodeNameVsNodeId.put(nodeInfo.getNode().getName(), nodeInfo.getNode().getId())
        );
    }
}
