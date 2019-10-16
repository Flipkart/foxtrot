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
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ConsoleV2;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v2/consoles")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v2/consoles")
public class ConsoleV2Resource {

    private ConsolePersistence consolePersistence;

    public ConsoleV2Resource(ConsolePersistence consolePersistence) {
        this.consolePersistence = consolePersistence;
    }

    @POST
    @Timed
    @ApiOperation("Save Console")
    public ConsoleV2 save(ConsoleV2 console) {
        consolePersistence.saveV2(console, true);
        return console;
    }

    @GET
    @Timed
    @Path("/{id}")
    @ApiOperation("Get Console - via id")
    public ConsoleV2 get(@PathParam("id") final String id) {
        return consolePersistence.getV2(id);
    }

    @DELETE
    @Path("/{id}/delete")
    @Timed
    @ApiOperation("Delete Console - via id")
    public void delete(@PathParam("id") final String id) {
        consolePersistence.deleteV2(id);
    }

    @GET
    @Timed
    @ApiOperation("Get all Consoles")
    public List<ConsoleV2> getList() {
        return consolePersistence.getV2();
    }

    @GET
    @Timed
    @Path("/{id}/old/get")
    @ApiOperation("get Old Version Console - via id")
    public ConsoleV2 getOldVersion(@PathParam("id") final String id) {
        return consolePersistence.getOldVersion(id);
    }

    @GET
    @Timed
    @Path("/{name}/old")
    @ApiOperation("Get All Old versions of console with name: {name}")
    public List<ConsoleV2> getOldVersions(@PathParam("name") final String name) {
        String sortBy = "updatedAt";
        return consolePersistence.getAllOldVersions(name, sortBy);
    }

    @DELETE
    @Path("/{id}/old/delete")
    @Timed
    @ApiOperation("Delete old version console - via id")
    public void deleteOldVersion(@PathParam("id") final String id) {
        consolePersistence.deleteOldVersion(id);
    }

    @GET
    @Timed
    @Path("/{id}/old/set/current")
    @ApiOperation("Set old version console with id: {id} as current console")
    public void setOldVersionAsCurrent(@PathParam("id") final String id) {
        consolePersistence.setOldVersionAsCurrent(id);
    }
}
