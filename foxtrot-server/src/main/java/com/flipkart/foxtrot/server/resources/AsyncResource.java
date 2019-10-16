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
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:09 PM
 */
@Path("/v1/async")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/async")
public class AsyncResource {

    private CacheManager cacheManager;

    public AsyncResource(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @GET
    @Path("/{action}/{id}")
    @Timed
    @ApiOperation("getResponse")
    public Response getResponse(@PathParam("action") final String action, @NotNull @PathParam("id") final String id) {
        return Response.ok(getData(new AsyncDataToken(action, id)))
                .build();
    }

    private ActionResponse getData(final AsyncDataToken dataToken) {
        return cacheManager.getCacheFor(dataToken.getAction())
                .get(dataToken.getKey());
    }

    @POST
    @Timed
    @ApiOperation("getResponsePost")
    public Response getResponsePost(final AsyncDataToken dataToken) {
        return Response.ok(getData(dataToken))
                .build();
    }
}
