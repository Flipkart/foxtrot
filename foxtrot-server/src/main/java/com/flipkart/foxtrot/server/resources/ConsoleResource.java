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

import com.flipkart.foxtrot.server.console.Console;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ConsolePersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Path("/v1/consoles")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ConsoleResource {
    private static final Logger logger = LoggerFactory.getLogger(ConsoleResource.class);

    private ConsolePersistence consolePersistence;

    public ConsoleResource(ConsolePersistence consolePersistence) {
        this.consolePersistence = consolePersistence;
    }

    @POST
    public Console save(Console console) {
        try {
            consolePersistence.save(console);
            return console;
        } catch (ConsolePersistenceException e) {
            logger.error("Error saving console: ", e);
            throw new WebApplicationException(Response.serverError()
                                                .entity(Collections.singletonMap("error", e.getMessage()))
                                                .build());
        }
    }

    @GET
    @Path("/{id}")
    public Console get(@PathParam("id") final String id) {
        try {
            return consolePersistence.get(id);
        } catch (ConsolePersistenceException e) {
            logger.error("Error getting console: ", e);
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", e.getMessage()))
                    .build());
        }
    }

    @DELETE
    @Path("/{id}/delete")
    public void delete(@PathParam("id") final String id) {
        try {
            consolePersistence.delete(id);
        } catch (ConsolePersistenceException e) {
            logger.error("Error deleting console: ", e);
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", e.getMessage()))
                    .build());
        }
    }

    @GET
    public List<Console> getList() {
        try {
            return consolePersistence.get();
        } catch (ConsolePersistenceException e) {
            logger.error("Error getting console list: ", e);
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", e.getMessage()))
                    .build());
        }
    }

}
