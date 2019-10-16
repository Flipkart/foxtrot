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

import com.flipkart.foxtrot.server.console.Console;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v1/consoles")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/consoles")
public class ConsoleResource {

    private ConsolePersistence consolePersistence;

    public ConsoleResource(ConsolePersistence consolePersistence) {
        this.consolePersistence = consolePersistence;
    }

    @POST
    @ApiOperation("Save Console")
    public Console save(Console console) {
        consolePersistence.save(console);
        return console;
    }

    @GET
    @Path("/{id}")
    @ApiOperation("Get Console - via id")
    public Console get(@PathParam("id") final String id) {
        return consolePersistence.get(id);
    }

    @DELETE
    @Path("/{id}/delete")
    @ApiOperation("Delete Console - via id")
    public void delete(@PathParam("id") final String id) {
        consolePersistence.delete(id);
    }

    @GET
    @ApiOperation("Get All Consoles")
    public List<Console> getList() {
        return consolePersistence.get();
    }

}
