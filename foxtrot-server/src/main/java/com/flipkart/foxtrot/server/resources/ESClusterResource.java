package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetailResponse;
import com.flipkart.foxtrot.common.nodegroup.visitors.MoveTablesRequest;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.rebalance.ClusterRebalanceService;
import com.flipkart.foxtrot.core.shardtuning.ShardCountTuningService;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.LocalDate;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.FORMATTER;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Slf4j
@Singleton
@Path("/v1/escluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/escluster")
public class ESClusterResource {

    private final ClusterRebalanceService clusterRebalanceService;
    private final ExecutorService executorService;
    private final NodeGroupManager nodeGroupManager;
    private final ShardCountTuningService shardCountTuningService;

    @Inject
    public ESClusterResource(final ClusterRebalanceService clusterRebalanceService,
                             final NodeGroupManager nodeGroupManager,
                             final ExecutorService executorService,
                             final ShardCountTuningService shardCountTuningService) {
        this.clusterRebalanceService = clusterRebalanceService;
        this.nodeGroupManager = nodeGroupManager;
        this.executorService = executorService;
        this.shardCountTuningService = shardCountTuningService;
    }

    @POST
    @Timed
    @Path("/rebalanceShards")
    @ApiOperation("rebalance shards")
    public Response rebalanceShards(@QueryParam("date") final String datePostFix) {
        if (!Strings.isNullOrEmpty(datePostFix) && !ElasticsearchUtils.isValidDatePostFix(datePostFix)) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(ImmutableMap.of("message", "Invalid date pattern. it should be - dd-M-yyyy"))
                    .build();
        }

        executorService.submit(() -> clusterRebalanceService.rebalanceShards(Strings.isNullOrEmpty(datePostFix)
                ? FORMATTER.print(LocalDate.now())
                : datePostFix));
        return Response.accepted()
                .build();
    }

    @POST
    @Timed
    @Path("/tuneShardCount")
    @ApiOperation("tune shard count for tables")
    public Response tuneShardCount() {
        executorService.submit((Runnable) shardCountTuningService::tuneShardCount);
        return Response.accepted()
                .build();
    }


    @POST
    @Timed
    @Path("/tuneShardCount/{table}")
    @ApiOperation("tune shard count for specific table")
    public Response tuneShardCount(@PathParam("table") String table) {
        executorService.submit(() -> shardCountTuningService.tuneShardCount(table));
        return Response.accepted()
                .build();
    }


    @GET
    @Timed
    @Path("/nodegroup")
    @ApiOperation("Get all ES node groups")
    public List<ESNodeGroup> getNodeGroups() {
        return nodeGroupManager.getNodeGroups();
    }


    @GET
    @Timed
    @Path("/nodegroup/details")
    @ApiOperation("Get all ES node group details")
    public List<ESNodeGroupDetailResponse> getNodeGroupDetails() {
        return nodeGroupManager.getNodeGroupDetails();
    }

    @POST
    @Timed
    @Path("/nodegroup")
    @ApiOperation("Create ES node group")
    public ESNodeGroup createNodeGroup(@Valid ESNodeGroup nodeGroup) {
        return nodeGroupManager.createNodeGroup(nodeGroup);
    }

    @GET
    @Timed
    @Path("/nodegroup/{groupName}")
    @ApiOperation("Get ES node group")
    public ESNodeGroup getNodeGroup(@PathParam("groupName") final String groupName) {
        return nodeGroupManager.getNodeGroup(groupName);
    }

    @GET
    @Timed
    @Path("/nodegroup/{groupName}/details")
    @ApiOperation("Get ES node group details")
    public ESNodeGroupDetailResponse getNodeGroupDetails(@PathParam("groupName") final String groupName) {
        return nodeGroupManager.getNodeGroupDetails(groupName);
    }

    @PUT
    @Timed
    @Path("/nodegroup/{groupName}")
    @ApiOperation("Update ES node group")
    public ESNodeGroup updateNodeGroup(@PathParam("groupName") final String groupName,
                                       @Valid ESNodeGroup nodeGroup) {
        return nodeGroupManager.updateNodeGroup(groupName, nodeGroup);
    }

    @PUT
    @Timed
    @Path("/nodegroup/{groupName}/sync")
    @ApiOperation("Sync ES node group allocation")
    public Response syncNodeGroup(@PathParam("groupName") final String groupName) {
        nodeGroupManager.syncAllocation(groupName);
        return Response.accepted()
                .build();
    }

    @DELETE
    @Timed
    @Path("/nodegroup/{groupName}")
    @ApiOperation("Delete ES node group")
    public Response deleteNodeGroup(@PathParam("groupName") final String groupName) {
        nodeGroupManager.deleteNodeGroup(groupName);
        return Response.accepted()
                .build();
    }

    @PUT
    @Timed
    @Path("/nodegroup/moveTables")
    @ApiOperation("Move Tables between ES node groups")
    public Response moveTablesBetweenGroups(@Valid MoveTablesRequest moveTablesRequest) {
        nodeGroupManager.moveTablesBetweenGroups(moveTablesRequest);
        return Response.accepted()
                .build();
    }
}
