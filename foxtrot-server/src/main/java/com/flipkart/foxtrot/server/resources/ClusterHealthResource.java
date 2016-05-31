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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

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
public class ClusterHealthResource {
    private final QueryStore queryStore;

    public ClusterHealthResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }


    @GET
    public ClusterHealthResponse getHealth() throws ExecutionException, InterruptedException, JsonProcessingException {
        return queryStore.getClusterHealth();
    }

    @GET
    @Path("/nodestats")
    public NodesStatsResponse getNodeStat() throws ExecutionException, InterruptedException, JsonProcessingException {
        return queryStore.getNodeStats();
    }

    @GET
    @Path("indicesstats")
    public IndicesStatsResponse getIndicesStat() throws ExecutionException, InterruptedException, JsonProcessingException {
        return queryStore.getIndicesStats();
    }
}
