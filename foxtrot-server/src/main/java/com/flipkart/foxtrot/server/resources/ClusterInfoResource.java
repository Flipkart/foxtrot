package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.cluster.ClusterMember;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Path("/v1/cluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/cluster")
public class ClusterInfoResource {

    private ClusterManager clusterManager;

    public ClusterInfoResource(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @GET
    @Path("/members")
    @Timed
    @ApiOperation("Get members")
    public Map<String, Collection<ClusterMember>> members() {
        return Collections.singletonMap("members", clusterManager.getMembers());
    }

}
