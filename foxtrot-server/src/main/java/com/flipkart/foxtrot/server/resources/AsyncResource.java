package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:09 PM
 */
@Path("/foxtrot/v1/async")
@Produces(MediaType.APPLICATION_JSON)
public class AsyncResource {
    @GET
    @Path("/{action}/{id}")
    public ActionResponse getResponse(@PathParam("action") final String action, @PathParam("id") final String id) {
        return getData(new AsyncDataToken(action, id));
    }

    @POST
    public ActionResponse getResponsePost(final AsyncDataToken dataToken) {
        return getData(dataToken);
    }

    private ActionResponse getData(final AsyncDataToken dataToken) {
        try {
            return Action.getResponse(dataToken);
        } catch (Exception e) {
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
    }
}
