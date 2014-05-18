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

@Path("/foxtrot/v1/consoles")
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
    public Console get(final String id) {
        try {
            return consolePersistence.get(id);
        } catch (ConsolePersistenceException e) {
            logger.error("Error getting console: ", e);
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
