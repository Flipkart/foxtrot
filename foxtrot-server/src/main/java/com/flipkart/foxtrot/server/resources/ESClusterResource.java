package com.flipkart.foxtrot.server.resources;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 Created by mudit.g on Sep, 2019
 ***/
@Singleton
@Path("/v1/escluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/escluster")
@Order(20)
public class ESClusterResource {

    private ClusterRerouteManager clusterRerouteManager;

    @Inject
    public ESClusterResource(ClusterRerouteManager clusterRerouteManager) {
        this.clusterRerouteManager = clusterRerouteManager;
    }

    /*@GET
    @Path("/reallocate")
    @ApiOperation("reallocate shards")
    public void reallocate() {
        clusterRerouteManager.reallocate();
    }*/
}
