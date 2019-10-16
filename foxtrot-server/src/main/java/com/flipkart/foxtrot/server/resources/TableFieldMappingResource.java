/**
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
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.stream.Collectors;

/**
 * Table metadata related apis
 */
@Path("/v1/tables")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/tables")
public class TableFieldMappingResource {

    private final TableManager tableManager;
    private final TableMetadataManager tableMetadataManager;

    public TableFieldMappingResource(TableManager tableManager, TableMetadataManager tableMetadataManager) {
        this.tableManager = tableManager;
        this.tableMetadataManager = tableMetadataManager;
    }

    @GET
    @Timed
    @Path("/{name}/fields")
    @ApiOperation("Get fields")
    public Response get(
            @PathParam("name") final String table,
            @QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality,
            @QueryParam("calculateCardinality") @DefaultValue("false") boolean calculateCardinality) {
        return Response.ok(tableMetadataManager.getFieldMappings(table, withCardinality, calculateCardinality))
                .build();
    }


    @GET
    @Timed
    @Path("/fields")
    @ApiOperation("Get all Fields")
    public Response getAllFields(
            @QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality,
            @QueryParam("calculateCardinality") @DefaultValue("false") boolean calculateCardinality) {
        return Response.ok()
                .entity(tableManager.getAll()
                                .stream()
                                .collect(Collectors.toMap(Table::getName,
                                                          table -> tableMetadataManager.getFieldMappings(
                                                                  table.getName(), withCardinality,
                                                                  calculateCardinality))))
                .build();
    }

    @POST
    @Timed
    @Path("/{name}/fields/update")
    @ApiOperation("Update Fields")
    public Response updateEstimation(
            @PathParam("name") final String table,
            @QueryParam("time") @DefaultValue("0") long epoch) {
        tableMetadataManager.updateEstimationData(table,
                                                  0 == epoch
                                                  ? System.currentTimeMillis()
                                                  : epoch);
        return Response.ok()
                .build();
    }
}
