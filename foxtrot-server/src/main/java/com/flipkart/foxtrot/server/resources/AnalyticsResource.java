/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:05 AM
 */
@Path("/v1/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AnalyticsResource {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsResource.class);
    private final QueryExecutor queryExecutor;

    public AnalyticsResource(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @POST
    public ActionResponse runSync(final ActionRequest request) {
        try {
            return queryExecutor.execute(request);
        } catch (QueryStoreException e) {
            logger.error(String.format("Error running sync request %s", request), e);
            switch (e.getErrorCode()) {
                case NO_SUCH_TABLE:
                    throw new WebApplicationException(
                            Response.status(Response.Status.BAD_REQUEST).entity(Collections.singletonMap("error", e.getMessage())).build());
                default:
                    throw new WebApplicationException(
                            Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
            }
        }
    }

    @POST
    @Path("/async")
    public AsyncDataToken runSyncAsync(final ActionRequest request) {
        try {
            return queryExecutor.executeAsync(request);
        } catch (QueryStoreException e) {
            logger.error(String.format("Error running async request %s", request), e);
            switch (e.getErrorCode()) {
                case NO_SUCH_TABLE:
                    throw new WebApplicationException(
                            Response.status(Response.Status.BAD_REQUEST).entity(Collections.singletonMap("error", e.getMessage())).build());
                default:
                    throw new WebApplicationException(
                            Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
            }
        }
    }
}
