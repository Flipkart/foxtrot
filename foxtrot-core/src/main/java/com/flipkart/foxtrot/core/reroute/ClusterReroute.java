package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.core.alerts.EmailClient;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.shard.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getTodayIndicesPattern;

/***
 Created by mudit.g on Feb, 2019
 ***/
public class ClusterReroute {
    private static final Logger logger = LoggerFactory.getLogger(ClusterReroute.class.getSimpleName());
    private ElasticsearchConnection connection;
    private Map<String, String> nodeNameVsNodeId;
    private Map<String, NodeInfo> nodeIdVsNodeInfoMap;
    private String fromNodeName;
    private EmailClient emailClient;
    private ClusterRerouteConfig clusterRerouteConfig;
    private LockingTaskExecutor executor;
    private Instant lockAtMostUntil;

    private static final String SUBJECT = "Shard Reallocation failed";

    public ClusterReroute(ElasticsearchConnection connection, String fromNodeName,
                          EmailClient emailClient, ClusterRerouteConfig clusterRerouteConfig,
                          LockingTaskExecutor executor, Instant lockAtMostUntil) {
        this.connection = connection;
        this.fromNodeName = fromNodeName;
        this.emailClient = emailClient;
        this.clusterRerouteConfig = clusterRerouteConfig;
        this.executor = executor;
        this.lockAtMostUntil = lockAtMostUntil;
        this.nodeNameVsNodeId = new HashMap<>();
        this.nodeIdVsNodeInfoMap = new HashMap<>();
    }

    public void run() {
        executor.executeWithLock(() -> {
            createNodeInfoMap();
            createNodeNameVsNodeIdMap();
            if (!nodeNameVsNodeId.containsKey(fromNodeName)) {
                emailClient.sendEmail(SUBJECT, "Could not resolve fromNodeName: " + fromNodeName, clusterRerouteConfig.getRecipients());
                return;
            }
            String fromNodeId = nodeNameVsNodeId.get(fromNodeName);
            if (!nodeIdVsNodeInfoMap.containsKey(fromNodeId)) {
                emailClient.sendEmail(SUBJECT, "Could not resolve fromNodeId: " + fromNodeId, clusterRerouteConfig.getRecipients());
                return;
            }
            LinkedList<Map.Entry<String, NodeInfo>> nodeInfoList = getSortedNodeInfoList();
            List<ShardInfo> shardInfoList = nodeIdVsNodeInfoMap.get(fromNodeId).getShardInfos();
            shardInfoList.sort(new Comparator<ShardInfo>() {
                @Override
                public int compare(ShardInfo o1, ShardInfo o2) {
                    return Long.compare(o1.getShardSize(), o2.getShardSize());
                }
            });
            int noOfRetries = clusterRerouteConfig.getNoOfRetries();
            int retryCount;
            for (retryCount = 0; retryCount < noOfRetries; retryCount++) {
                if ((nodeInfoList.size() -1 -retryCount) < 0) {
                    break;
                }
                String toNodeId = nodeInfoList.get(nodeInfoList.size() -1 -retryCount).getKey();
                boolean reallocationSuccessful = true;
                for (int i = 0; i < clusterRerouteConfig.getNoOfShardsToBeReallocated(); i++) {
                    if ((shardInfoList.size() -1 -i) < 0) {
                        break;
                    }
                    ShardId shardId = shardInfoList.get(shardInfoList.size() -1 -i).getShardId();
                    if (!shardReallocation(shardId, fromNodeId, toNodeId)) {
                        reallocationSuccessful = false;
                    }
                }
                if (reallocationSuccessful) {
                    return;
                }
            }
            if (retryCount == noOfRetries) {
                emailClient.sendEmail(SUBJECT, "Exceeded max no. of retries: " + noOfRetries, clusterRerouteConfig.getRecipients());
            }
        }, new LockConfiguration(ClusterRerouteConfig.getJobName(), lockAtMostUntil));
    }

    private boolean shardReallocation(ShardId shardId, String fromNode, String toNode) {
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(shardId, fromNode, toNode);
        ClusterRerouteRequest clusterRerouteRequest = new ClusterRerouteRequest();
        clusterRerouteRequest.add(moveAllocationCommand);
        try {
            ClusterRerouteResponse clusterRerouteResponse = connection.getClient()
                    .admin()
                    .cluster()
                    .reroute(clusterRerouteRequest)
                    .actionGet();
            logger.info(String.format("Reallocating Shard. From Node: %s To Node: %s", fromNode, toNode));
            return clusterRerouteResponse.isAcknowledged();
        } catch (Exception e) {
            logger.error(String.format("Error in reallocating Shard. From Node: %s To Node: %s", fromNode, toNode), e);
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
                            List<ShardInfo> shardInfoList = new ArrayList<>(Arrays.asList(shardInfo));
                            NodeInfo nodeInfo = new NodeInfo(shardSize, shardInfoList);
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
