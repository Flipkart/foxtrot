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
import com.flipkart.foxtrot.common.TableV2;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.gandalf.manager.GandalfManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@Path("/v2/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v2/tables")
public class TableManagerV2Resource {

    private final TableManager tableManager;
    private final GandalfManager gandalfManager;

    public TableManagerV2Resource(TableManager tableManager, GandalfManager gandalfManager) {
        this.tableManager = tableManager;
        this.gandalfManager = gandalfManager;
    }

    private static Table toWireModel(TableV2 table) {
        return Table.builder()
                .name(table.getName())
                .ttl(table.getTtl())
                .build();
    }

    @POST
    @Timed
    @ApiOperation("Save Table")
    public Response save(
            @Valid final TableV2 table,
            @QueryParam("forceCreate") @DefaultValue("false") boolean forceCreate) {
        table.setName(ElasticsearchUtils.getValidTableName(table.getName()));
        tableManager.save(toWireModel(table), forceCreate);
        gandalfManager.manage(table);
        return Response.ok(table)
                .build();
    }

    @GET
    @Timed
    @Path("/{name}")
    @ApiOperation("Get Table")
    public Response get(@PathParam("name") String name) {
        name = ElasticsearchUtils.getValidTableName(name);
        Table table = tableManager.get(name);
        return Response.ok()
                .entity(table)
                .build();
    }

    @PUT
    @Timed
    @Path("/{name}")
    @ApiOperation("Update Table")
    public Response get(@PathParam("name") final String name, @Valid final Table table) {
        table.setName(name);
        tableManager.update(table);
        return Response.ok()
                .build();
    }

    @DELETE
    @Timed
    @Path("/{name}/delete")
    @ApiOperation("Delete Table")
    public Response delete(@PathParam("name") String name) {
        name = ElasticsearchUtils.getValidTableName(name);
        tableManager.delete(name);
        return Response.status(Response.Status.NO_CONTENT)
                .build();
    }

    @GET
    @Timed
    @ApiOperation("Get all Tables")
    public Response getAll() {
        return Response.ok()
                .entity(tableManager.getAll())
                .build();
    }
}
