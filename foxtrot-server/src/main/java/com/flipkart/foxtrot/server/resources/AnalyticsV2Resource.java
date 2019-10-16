package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.gandalf.access.AccessService;
import com.flipkart.foxtrot.server.config.QueryConfig;
import com.phonepe.gandalf.client.annotation.GandalfUserContext;
import com.phonepe.gandalf.models.user.UserDetails;
import io.dropwizard.primer.auth.annotation.Authorize;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class AnalyticsV2Resource {

    private static final String AUTHORIZATION_EXCEPTION_MESSAGE = "User not Authorised";
    private final QueryExecutor queryExecutor;
    private final AccessService accessService;
    private final QueryConfig queryConfig;

    public AnalyticsV2Resource(QueryExecutor queryExecutor, AccessService accessService, QueryConfig queryConfig) {
        this.queryExecutor = queryExecutor;
        this.accessService = accessService;
        this.queryConfig = queryConfig;
    }

    @POST
    @Timed
    @ApiOperation("runSync")
    @Authorize(value = {})
    public ActionResponse runSync(@Valid final ActionRequest request, @GandalfUserContext UserDetails userDetails) {
        preprocess(request, userDetails);
        return queryExecutor.execute(request);
    }

    @POST
    @Path("/async")
    @Timed
    @ApiOperation("runSyncAsync")
    @Authorize(value = {})
    public AsyncDataToken runSyncAsync(
            @Valid final ActionRequest request,
            @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                                                                     new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        }
        catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        return queryExecutor.executeAsync(request);
    }

    @POST
    @Path("/validate")
    @Timed
    @ApiOperation("validateQuery")
    @Authorize(value = {})
    public ActionValidationResponse validateQuery(
            @Valid final ActionRequest request,
            @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                                                                     new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        }
        catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        return queryExecutor.validate(request);
    }

    private void preprocess(@Valid ActionRequest request, @GandalfUserContext UserDetails userDetails) {
        try {
            if (!accessService.hasAccess(request, userDetails)) {
                throw FoxtrotExceptions.createAuthorizationException(request,
                                                                     new Exception(AUTHORIZATION_EXCEPTION_MESSAGE));
            }
        }
        catch (Exception e) {
            throw FoxtrotExceptions.createAuthorizationException(request, e);
        }
        if (queryConfig.isBlockConsoleQueries()) {
            log.info("Query is blocked because of high load " + request);
            throw FoxtrotExceptions.createConsoleQueryBlockedException(request);
        }
        if (queryConfig.isLogQueries()) {
            if (ElasticsearchUtils.isTimeFilterPresent(request.getFilters())) {
                log.info("Console Query : " + request.toString());
            }
            else {
                log.info("Console Query where time filter is not specified, request: {}", request.toString());
            }
        }
    }
}
