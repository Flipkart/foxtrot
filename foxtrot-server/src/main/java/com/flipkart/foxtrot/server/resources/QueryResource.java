package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.FilterEventsAction;
import com.flipkart.foxtrot.core.querystore.actions.QueryResponse;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 11:23 PM
 */
@Path("/foxtrot/v1/query")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {
    private QueryStore queryStore;

    public QueryResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @POST
    public QueryResponse runQuery(@Valid final Query query) {
        try {
            return queryStore.runQuery(query);
        } catch (QueryStoreException e) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
    }

    @POST
    @Path("/async")
    public String runQueryAsync(@Valid final Query query) {
        try {
            return queryStore.runQueryAsync(query);
        } catch (Exception e) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
    }

    @GET
    @Path("/{id}")
    public QueryResponse getResponse(@PathParam("{id}") final String id) {
        try {
            Cache<QueryResponse> cache = CacheUtils.getCacheFor(FilterEventsAction.class.getCanonicalName());
            if(cache.has(id))
                return cache.get(id);
        } catch (Exception e) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
        throw new WebApplicationException(Response.noContent().build());
    }
}
