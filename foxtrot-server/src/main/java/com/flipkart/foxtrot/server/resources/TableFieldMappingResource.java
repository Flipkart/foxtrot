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
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.stream.Collectors;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
@Path("/v1/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {

    private final TableManager tableManager;
    private final QueryStore queryStore;

    public TableFieldMappingResource(TableManager tableManager, QueryStore queryStore) {
        this.tableManager = tableManager;
        this.queryStore = queryStore;
    }

    @GET
    @Path("/{name}/fields")
    public Response get(@PathParam("name") final String table) throws FoxtrotException {
        return Response.ok(queryStore.getFieldMappings(table)).build();
    }


    @GET
    @Path("/fields")
    public Response getAllFields() throws FoxtrotException {
        return Response.ok()
                .entity(tableManager.getAll()
                        .stream()
                        .collect(
                                Collectors.toMap(Table::getName, table -> {
                                    try {
                                        return queryStore.getFieldMappings(table.getName());
                                    } catch (FoxtrotException e) {
                                        throw new RuntimeException(e);
                                    }
                                })))
                .build();
    }
}
