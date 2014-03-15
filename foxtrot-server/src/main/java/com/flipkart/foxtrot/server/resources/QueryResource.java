package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 11:23 PM
 */
@Path("/foxtrot/v1/query")
public class QueryResource {
    private static final Logger logger = LoggerFactory.getLogger(QueryResource.class.getSimpleName());

    private QueryStore queryStore;

    public QueryResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @GET
    public Response runQuery(@PathParam("table")final String table, final Query query) {
        try {
            return Response.ok(queryStore.runQuery(query)).build();
        } catch (Exception e) {
            logger.error("Error saving document: ", e);
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build();
        }
    }
}
