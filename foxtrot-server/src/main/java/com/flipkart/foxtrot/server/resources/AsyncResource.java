package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.sun.istack.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:09 PM
 */
@Path("/v1/async")
@Produces(MediaType.APPLICATION_JSON)
public class AsyncResource {
    @GET
    @Path("/{action}/{id}")
    public Response getResponse(@PathParam("action") final String action, @NotNull @PathParam("id") final String id) {
        return Response.ok(getData(new AsyncDataToken(action, id))).build();
    }

    @POST
    public Response getResponsePost(final AsyncDataToken dataToken) {
        return Response.ok(getData(dataToken)).build();
    }

    private ActionResponse getData(final AsyncDataToken dataToken) {
        try {
            return CacheUtils.getCacheFor(dataToken.getAction()).get(dataToken.getKey());
        } catch (Exception e) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
    }
}
