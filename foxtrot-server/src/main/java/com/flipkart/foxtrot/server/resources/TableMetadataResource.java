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
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Path("/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableMetadataResource {
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataResource.class);
    private final TableMetadataManager tableMetadataManager;

    public TableMetadataResource(TableMetadataManager tableMetadataManager) {
        this.tableMetadataManager = tableMetadataManager;
    }

    @POST
    public Table save(@Valid final Table table) {
        try {
            tableMetadataManager.save(table);
        } catch (Exception e) {
            logger.error(String.format("Unable to save table %s", table), e);
            throw new WebApplicationException(Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        }
        return table;
    }

    @GET
    @Path("/{name}")
    public Table get(@PathParam("name") final String name) throws Exception {
        Table table = tableMetadataManager.get(name);
        if (table == null) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
        return table;
    }

    @DELETE
    @Path("/{name}/delete")
    public void delete(@PathParam("name") final String name) throws Exception {
        try {
            tableMetadataManager.delete(name);
        } catch (Exception ex){
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", ex.getMessage()))
                    .build());
        }
    }

    @GET
    public List<Table> get() throws Exception {
        return tableMetadataManager.get();
    }
}
