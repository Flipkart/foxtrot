package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.reroute.ClusterRerouteManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Singleton
@Path("/v1/escluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/escluster")
public class ESClusterResource {

    private ClusterRerouteManager clusterRerouteManager;

    @Inject
    public ESClusterResource(ClusterRerouteManager clusterRerouteManager) {
        this.clusterRerouteManager = clusterRerouteManager;
    }

    @GET
    @Path("/reallocate")
    @ApiOperation("reallocate shards")
    public void reallocate() {
        clusterRerouteManager.reallocate();
    }
}
