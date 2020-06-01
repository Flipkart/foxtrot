package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.reroute.ClusterRerouteManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Path("/v1/escluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/escluster")
@Singleton
@Order(20)
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
