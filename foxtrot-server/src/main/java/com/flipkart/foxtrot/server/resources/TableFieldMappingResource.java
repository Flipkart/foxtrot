/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.inject.Singleton;
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
@Singleton
@PermitAll
public class TableFieldMappingResource {

    private final TableManager tableManager;
    private final TableMetadataManager tableMetadataManager;

    @Inject
    public TableFieldMappingResource(TableManager tableManager,
                                     TableMetadataManager tableMetadataManager) {
        this.tableManager = tableManager;
        this.tableMetadataManager = tableMetadataManager;
    }

    @GET
    @Timed
    @Path("/{name}/fields")
    @ApiOperation("Get fields")
    public Response get(@PathParam("name") final String table,
                        @QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality) {
        TableFieldMapping tableFieldMapping = getTableFieldMapping(table, withCardinality);
        return Response.ok(tableFieldMapping)
                .build();
    }

    private TableFieldMapping getTableFieldMapping(String table,
                                                   boolean withCardinality) {
        TableFieldMapping tableFieldMapping;
        if (withCardinality) {
            tableFieldMapping = tableMetadataManager.getFieldMappingsWithCardinality(table);
        } else {
            tableFieldMapping = tableMetadataManager.getFieldMappings(table);
        }
        return tableFieldMapping;
    }


    @GET
    @Timed
    @Path("/fields")
    @ApiOperation("Get all Fields")
    public Response getAllFields(@QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality) {
        return Response.ok()
                .entity(tableManager.getAll()
                        .stream()
                        .collect(Collectors.toMap(Table::getName,
                                table -> getTableFieldMapping(table.getName(), withCardinality))))
                .build();
    }

    @POST
    @Timed
    @Path("/{name}/fields/update")
    @ApiOperation("Update Fields")
    public Response updateEstimation(@PathParam("name") final String table,
                                     @QueryParam("time") @DefaultValue("0") long epoch) {
        tableMetadataManager.updateEstimationData(table, 0 == epoch
                ? System.currentTimeMillis()
                : epoch);
        return Response.ok()
                .build();
    }
}
