/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.server.console.QueryManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/querymanager")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/querymanager")
@Singleton
public class QueryManagerResource {

    private QueryManager queryManager;

    @Inject
    public QueryManagerResource(final QueryManager queryManager) {
        this.queryManager = queryManager;
    }

    @PUT
    @Timed
    @Path("/console/{consoleId}")
    @ApiOperation("Enabled or Disable queries from a console id")
    public Response manageQueriesFromConsole(@PathParam("consoleId") final String consoleId,
                                             @QueryParam("enabled") final boolean enabled) {
        if (!enabled) {
            queryManager.blacklistConsole(consoleId);
        } else {
            queryManager.whitelistConsole(consoleId);
        }
        return Response.accepted()
                .build();
    }

    @PUT
    @Timed
    @Path("/user/{userId}")
    @ApiOperation("Enabled or Disable queries from a user id")
    public Response manageQueriesFromUser(@PathParam("userId") final String userId,
                                          @QueryParam("enabled") final boolean enabled) {
        if (!enabled) {
            queryManager.blacklistUserId(userId);
        } else {
            queryManager.whitelistUserId(userId);
        }
        return Response.accepted()
                .build();
    }

    @PUT
    @Timed
    @Path("/sourceType/{sourceType}")
    @ApiOperation("Enabled or Disable queries from a source type")
    public Response manageQueriesFromSourceType(@PathParam("sourceType") final SourceType sourceType,
                                                @QueryParam("enabled") final boolean enabled) {
        if (!enabled) {
            queryManager.blacklistSourceType(sourceType.name());
        } else {
            queryManager.whitelistSourceType(sourceType.name());
        }
        return Response.accepted()
                .build();
    }
}
