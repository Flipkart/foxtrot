/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

@Path("/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableManagerResource {
    private static final Logger logger = LoggerFactory.getLogger(TableManagerResource.class);
    private final TableManager tableManager;

    public TableManagerResource(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    @POST
    public Response save(@Valid final Table table) {
        try {
            tableManager.save(table);
            return Response.ok(table).build();
        } catch (TableManagerException e) {
            logger.error(String.format("Unable to save table %s", table), e);
            switch (e.getErrorCode()) {
                case TABLE_ALREADY_EXISTS:
                    return Response.status(422).entity(Collections.singletonMap("error", e.getMessage())).build();
                case INTERNAL_ERROR:
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(Collections.singletonMap("error", e.getMessage())).build();
                default:
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(Collections.singletonMap("error", e.getMessage())).build();
            }
        }
    }

    @GET
    @Path("/{name}")
    public Response get(@PathParam("name") final String name) {
        try {
            Table table = tableManager.get(name);
            return Response.ok().entity(table).build();
        } catch (TableManagerException e) {
            logger.error(String.format("Unable to fetch table %s", name), e);
            switch (e.getErrorCode()) {
                case TABLE_NOT_FOUND:
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(Collections.singletonMap("error", e.getMessage())).build();
                case INTERNAL_ERROR:
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(Collections.singletonMap("error", e.getMessage())).build();
                default:
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(Collections.singletonMap("error", e.getMessage())).build();
            }
        }
    }

    @DELETE
    @Path("/{name}/delete")
    public Response delete(@PathParam("name") final String name) {
        try {
            tableManager.delete(name);
            return Response.status(Response.Status.NO_CONTENT).build();
        } catch (Exception e) {
            logger.error(String.format("Unable to delete table %s", name), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Collections.singletonMap("error", e.getMessage())).build();
        }
    }

    @GET
    public Response getAll() {
        try {
            return Response.ok().entity(tableManager.getAll()).build();
        } catch (TableManagerException e) {
            logger.error("Unable to fetch table list", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Collections.singletonMap("error", e.getMessage())).build();
        }
    }
}
