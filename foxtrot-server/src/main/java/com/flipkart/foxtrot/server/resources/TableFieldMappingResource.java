package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
@Path("/v1/tables/{name}/fields")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {
    private static final Logger logger = LoggerFactory.getLogger(TableFieldMappingResource.class.getSimpleName());
    private QueryStore queryStore;

    public TableFieldMappingResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @GET
    public Response get(@PathParam("name") final String table) {
        try {
            return Response.ok(queryStore.getFieldMappings(table)).build();
        } catch (QueryStoreException ex) {
            logger.error("Unable to fetch Table Metadata " , ex);
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Metadata Fetch Failed")).build());
        }
    }

}
