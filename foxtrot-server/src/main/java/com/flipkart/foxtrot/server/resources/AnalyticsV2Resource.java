package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutorFactory;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.server.console.QueryManager;
import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.responseprocessors.Flattener;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.appform.functionmetrics.MonitoredFunction;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/***
 Created by mudit.g on Mar, 2019
 ***/
@Singleton
@Path("/v2/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v2/analytics")
@Slf4j
public class AnalyticsV2Resource {

    private final QueryExecutorFactory executorFactory;
    private final QueryConfig queryConfig;
    private final ObjectMapper objectMapper;
    private final QueryManager queryManager;

    @Inject
    public AnalyticsV2Resource(final QueryExecutorFactory executorFactory,
                               final ObjectMapper objectMapper,
                               final QueryConfig queryConfig,
                               final QueryManager queryManager) {
        this.executorFactory = executorFactory;
        this.queryConfig = queryConfig;
        this.objectMapper = objectMapper;
        this.queryManager = queryManager;
    }

    @POST
    @Timed
    @ApiOperation("runSync")
    public ActionResponse runSync(@Valid final ActionRequest request) {
        preprocess(request);
        return executorFactory.getExecutor(request)
                .execute(request);
    }

    @POST
    @Path("/async")
    @Timed
    @ApiOperation("runSyncAsync")
    public AsyncDataToken runSyncAsync(@Valid final ActionRequest request) {
        preprocess(request);

        return executorFactory.getExecutor(request)
                .executeAsync(request);
    }

    @POST
    @Path("/validate")
    @Timed
    @ApiOperation("validateQuery")
    public ActionValidationResponse validateQuery(@Valid final ActionRequest request) {
        preprocess(request);

        return executorFactory.getExecutor(request)
                .validate(request);
    }

    @POST
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download")
    @Timed
    @ApiOperation("downloadAnalytics")
    public StreamingOutput download(@Valid final ActionRequest actionRequest) {
        preprocess(actionRequest);
        ActionResponse actionResponse = executorFactory.getExecutor(actionRequest)
                .execute(actionRequest);
        Flattener flattener = new Flattener(objectMapper, actionRequest, new ArrayList<>());
        FlatRepresentation flatRepresentation = actionResponse.accept(flattener);
        return output -> FlatToCsvConverter.convert(flatRepresentation, new OutputStreamWriter(output));
    }

    @MonitoredFunction
    private void preprocess(@Valid ActionRequest request) {

        if (queryConfig.isLogQueries()) {
            if (ElasticsearchUtils.isTimeFilterPresent(request.getFilters())) {
                log.info("Console Query : " + JsonUtils.toJson(request));
            } else {
                log.info("Console Query where time filter is not specified, request: {}", JsonUtils.toJson(request));
            }
        }

    }
}
