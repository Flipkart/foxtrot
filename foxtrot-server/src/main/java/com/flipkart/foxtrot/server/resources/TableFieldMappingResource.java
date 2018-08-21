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

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.stream.Collectors;

/**
 * Table metadata related apis
 */
@Path("/v1/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {

    private final TableManager tableManager;
    private final TableMetadataManager tableMetadataManager;

    public TableFieldMappingResource(TableManager tableManager, TableMetadataManager tableMetadataManager) {
        this.tableManager = tableManager;
        this.tableMetadataManager = tableMetadataManager;
    }

    @GET
    @Path("/{name}/fields")
    public Response get(@PathParam("name") final String table,
                        @QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality,
                        @QueryParam("calculateCardinality") @DefaultValue("false") boolean calculateCardinality) throws FoxtrotException {
        return Response.ok(tableMetadataManager.getFieldMappings(table, withCardinality, calculateCardinality)).build();
    }


    @GET
    @Path("/fields")
    public Response getAllFields(@QueryParam("withCardinality") @DefaultValue("false") boolean withCardinality,
                                 @QueryParam("calculateCardinality") @DefaultValue("false") boolean calculateCardinality) throws FoxtrotException {
        return Response.ok()
                .entity(tableManager.getAll()
                        .stream()
                        .collect(
                                Collectors.toMap(Table::getName, table -> {
                                    try {
                                        return tableMetadataManager.getFieldMappings(table.getName(), withCardinality, calculateCardinality);
                                    } catch (FoxtrotException e) {
                                        throw new RuntimeException(e);
                                    }
                                })))
                .build();
    }

    @POST
    @Path("/{name}/fields/update")
    public Response updateEstimation(
            @PathParam("name") final String table,
            @QueryParam("time") @DefaultValue("0") long epoch) throws FoxtrotException {
        tableMetadataManager.updateEstimationData(table, 0 == epoch ? System.currentTimeMillis() : epoch);
        return Response.ok()
                .build();
    }
}
