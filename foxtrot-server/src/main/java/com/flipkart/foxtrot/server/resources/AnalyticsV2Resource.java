package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.gandalf.access.AccessService;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.phonepe.gandalf.client.annotation.GandalfUserContext;
import com.phonepe.gandalf.models.user.UserDetails;
import io.dropwizard.primer.auth.annotation.Authorize;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/***
 Created by mudit.g on Mar, 2019
 ***/
@Path("/v2/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v2/analytics")
public class AnalyticsV2Resource {

    private static final String AUTHORIZATION_EXCEPTION_MESSAGE = "User not Authorised";
    private final QueryExecutor queryExecutor;
    private final AccessService accessService;

    public AnalyticsV2Resource(QueryExecutor queryExecutor, AccessService accessService) {
        this.queryExecutor = queryExecutor;
        this.accessService = accessService;
    }

    @POST
    @Timed
    @ApiOperation("runSync")
    @Authorize(value = {})
    public ActionResponse runSync(@Valid final ActionRequest request, @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                        new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        } catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        return queryExecutor.execute(request, userDetails.getEmail());
    }

    @POST
    @Path("/async")
    @Timed
    @ApiOperation("runSyncAsync")
    @Authorize(value = {})
    public AsyncDataToken runSyncAsync(@Valid final ActionRequest request,
            @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                        new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        } catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        return queryExecutor.executeAsync(request, userDetails.getEmail());
    }

    @POST
    @Path("/validate")
    @Timed
    @ApiOperation("validateQuery")
    @Authorize(value = {})
    public ActionValidationResponse validateQuery(@Valid final ActionRequest request,
            @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                        new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        } catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        return queryExecutor.validate(request, userDetails.getEmail());
    }
}
