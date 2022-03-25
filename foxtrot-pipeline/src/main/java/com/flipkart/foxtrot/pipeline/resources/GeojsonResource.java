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
package com.flipkart.foxtrot.pipeline.resources;

import com.codahale.metrics.annotation.Timed;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Slf4j
@Path("/v1/geojson")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/geojson")
@Singleton
public class GeojsonResource {

    private GeojsonStore store;

    @Inject
    public GeojsonResource(final GeojsonStore store) {
        this.store = store;
    }

    @POST
    @Timed
    @Path("/{collectionId}")
    @ApiOperation("Fetch features from a Geojson collection")
    public Response fetch(@PathParam("collectionId") String collectionId,
                          @Valid final GeojsonFetchRequest request) {
        val features = store.fetch(collectionId, request.getIds());
        return Response.ok(features)
                .build();
    }
}
