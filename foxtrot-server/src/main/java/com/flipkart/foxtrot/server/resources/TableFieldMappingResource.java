package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
@Path("/foxtrot/v1/fields/{table}")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {

    private QueryStore queryStore;

    public TableFieldMappingResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @GET
    public Response get(@PathParam("table") final String table) {
        try {
            return Response.ok(queryStore.getFieldMappings(table)).build();
        } catch (QueryStoreException ex) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Metadata Fetch Failed")).build());
        }
    }

}
