/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

/**
 * Created by swapnil on 20/01/16.
 */

@Path("/v1/clusterhealth")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/clusterhealth")
@Singleton
public class ClusterHealthResource {
    private final QueryStore queryStore;

    @Inject
    public ClusterHealthResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }


    @GET
    @Timed
    @ApiOperation("getHealth")
    public ClusterHealthResponse getHealth() throws ExecutionException, InterruptedException {
        return queryStore.getClusterHealth();
    }

    @GET
    @Timed
    @Path("/nodestats")
    @ApiOperation("getNodeStat")
    public NodesStatsResponse getNodeStat() throws ExecutionException, InterruptedException {
        return queryStore.getNodeStats();
    }

    @GET
    @Timed
    @Path("indicesstats")
    @ApiOperation("getIndicesStat")
    public IndicesStatsResponse getIndicesStat() throws ExecutionException, InterruptedException {
        return queryStore.getIndicesStats();
    }
}
