/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import io.dropwizard.auth.Auth;
import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.responseprocessors.Flattener;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:05 AM
 */
@Path("/v1/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/analytics")
@Singleton
@RolesAllowed(FoxtrotRole.Value.QUERY)
public class AnalyticsResource {

    private final QueryExecutor queryExecutor;
    private final ObjectMapper objectMapper;

    @Inject
    public AnalyticsResource(final QueryExecutor queryExecutor, final ObjectMapper objectMapper) {
        this.queryExecutor = queryExecutor;
        this.objectMapper = objectMapper;
    }

    @POST
    @Timed
    @ApiOperation("runSync")
    @RolesAllowed(FoxtrotRole.Value.QUERY)
    public ActionResponse runSync(
            @Auth final UserPrincipal userPrincipal,
            @Valid final ActionRequest request) {
        return queryExecutor.execute(request);
    }

    @POST
    @Path("/async")
    @Timed
    @ApiOperation("runSyncAsync")
    @RolesAllowed(FoxtrotRole.Value.QUERY)
    public AsyncDataToken runSyncAsync(
            @Auth final UserPrincipal userPrincipal,
            @Valid final ActionRequest request) {
        return queryExecutor.executeAsync(request);
    }

    @POST
    @Path("/validate")
    @Timed
    @ApiOperation("validateQuery")
    public ActionValidationResponse validateQuery(@Valid final ActionRequest request) {
        return queryExecutor.validate(request);
    }

    @POST
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download")
    @Timed
    @ApiOperation("downloadAnalytics")
    public StreamingOutput download(@Valid final ActionRequest actionRequest) {
        ActionResponse actionResponse = queryExecutor.execute(actionRequest);
        Flattener flattener = new Flattener(objectMapper, actionRequest, new ArrayList<>());
        actionResponse.accept(flattener);
        return output -> FlatToCsvConverter.convert(flattener.getFlatRepresentation(), new OutputStreamWriter(output));
    }
}
