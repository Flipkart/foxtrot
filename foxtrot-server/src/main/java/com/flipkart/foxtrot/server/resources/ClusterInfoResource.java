package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.cluster.ClusterMember;
import com.hazelcast.core.Member;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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


    @GET
    @Path("/hazelcast/members")
    @Timed
    @ApiOperation("Get Hazelcast cluster members")
    public Map<String, Collection<Member>> hazelcastMembers() {
        return Collections.singletonMap("members", clusterManager.getHazelcastMembers());
    }
}
