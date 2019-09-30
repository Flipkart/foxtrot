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
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.server.config.QueryConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:05 AM
 */
@Path("/v1/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/analytics")
@Slf4j
public class AnalyticsResource {

    private final QueryExecutor queryExecutor;
    private final QueryConfig queryConfig;

    public AnalyticsResource(QueryExecutor queryExecutor, QueryConfig queryConfig) {
        this.queryExecutor = queryExecutor;
        this.queryConfig = queryConfig;
    }

    @POST
    @Timed
    @ApiOperation("runSync")
    public ActionResponse runSync(@Valid final ActionRequest request) {
        preprocess(request);
        return queryExecutor.execute(request, "");
    }

    @POST
    @Path("/async")
    @Timed
    @ApiOperation("runSyncAsync")
    public AsyncDataToken runSyncAsync(@Valid final ActionRequest request) {
        return queryExecutor.executeAsync(request, "");
    }

    @POST
    @Path("/validate")
    @Timed
    @ApiOperation("validateQuery")
    public ActionValidationResponse validateQuery(@Valid final ActionRequest request) {
        return queryExecutor.validate(request, "");
    }

    private void preprocess(ActionRequest request) {
        if (queryConfig.isLogQueries()){
            if (ElasticsearchUtils.isTimeFilterPresent(request.getFilters())) {
                log.info("Analytics Query");
            } else {
                log.info("Analytics Query where time filter is not specified, request: {}", request.toString());
            }
        }
    }
}
