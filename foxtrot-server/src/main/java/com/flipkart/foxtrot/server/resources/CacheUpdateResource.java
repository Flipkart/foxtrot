package com.flipkart.foxtrot.server.resources;/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationRunnable;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutorService;

/***
 Created by nitish.goyal on 17/08/18
 ***/
@Path("/v1/cache/update")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/cache/update")
@Singleton
public class CacheUpdateResource {
    private ExecutorService executorService;
    private TableMetadataManager tableMetadataManager;

    @Inject
    public CacheUpdateResource(
            ExecutorService executorService,
            TableMetadataManager tableMetadataManager) {
        this.executorService = executorService;
        this.tableMetadataManager = tableMetadataManager;
    }

    @Path("/cardinality")
    @Timed
    @POST
    @ApiOperation("updateCardinalityCache")
    public Response updateCardinalityCache() {
        executorService.submit(new CardinalityCalculationRunnable(tableMetadataManager));
        return Response.ok()
                .build();
    }

}
