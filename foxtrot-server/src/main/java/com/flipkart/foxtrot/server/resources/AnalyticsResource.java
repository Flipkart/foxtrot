package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.common.ActionResponse;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:05 AM
 */
@Path("/foxtrot/v1/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AnalyticsResource {
    private QueryExecutor queryExecutor;

    public AnalyticsResource(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @POST
    public ActionResponse runSync(final ActionRequest request) {
        try {
            return queryExecutor.execute(request);
        } catch (QueryStoreException e) {
            throw new WebApplicationException(
                        Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        }
    }
}
