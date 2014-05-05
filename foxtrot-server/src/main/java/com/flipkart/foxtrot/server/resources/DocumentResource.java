package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.sun.istack.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:55 PM
 */
@Path("/foxtrot/v1/document/{table}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DocumentResource {
    private static final Logger logger = LoggerFactory.getLogger(DocumentResource.class.getSimpleName());

    private final QueryStore queryStore;

    public DocumentResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @POST
    public Response saveDocument(@PathParam("table") final String table, @Valid final Document document) {
        try {
            queryStore.save(table, document);
        } catch (Exception e) {
            logger.error("Error saving document: ", e);
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build();
        }
        return Response.created(URI.create("/" + document.getId())).build();
    }

    @POST
    @Path("/bulk")
    public Response saveDocuments(@PathParam("table") final String table, @Valid final List<Document> document) {
        try {
            queryStore.save(table, document);
        } catch (Exception e) {
            logger.error("Error saving document: ", e);
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build();
        }
        return Response.created(URI.create("/" + table)).build();
    }

    @GET
    @Path("/{id}")
    public Response getDocument(@PathParam("table") final String table, @PathParam("id") @NotNull final String id) {
        try {
            return Response.ok(queryStore.get(table, id)).build();
        } catch (Exception e) {
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not get document: " + e.getMessage()))
                    .build();
        }
    }

    @GET
    public Response getDocuments(@PathParam("table") final String table, @QueryParam("id") @NotNull final List<String> ids) {
        System.out.println("Ids are : " + ids);
        try {
            return Response.ok(queryStore.get(table, ids)).build();
        } catch (Exception e) {
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not get document: " + e.getMessage()))
                    .build();
        }
    }
}
