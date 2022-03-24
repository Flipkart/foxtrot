package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.FqlRequest;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.exception.BadRequestException;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.server.console.QueryManager;
import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlGetRequest;
import com.flipkart.foxtrot.sql.fqlstore.FqlStore;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import static com.flipkart.foxtrot.common.headers.FoxtrotRequestInfoHeaders.*;

/***
 Created by mudit.g on Oct, 2019
 ***/
@Singleton
@Path("/v2/fql")
@Api(value = "/v2/fql")
@Slf4j
public class FqlV2Resource {

    private final QueryConfig queryConfig;
    private final FqlEngine fqlEngine;
    private final FqlStoreService fqlStoreService;
    private final QueryManager queryManager;

    @Inject
    public FqlV2Resource(final FqlEngine fqlEngine,
                         final FqlStoreService fqlStoreService,
                         final QueryConfig queryConfig,
                         final QueryManager queryManager) {
        this.fqlEngine = fqlEngine;
        this.fqlStoreService = fqlStoreService;
        this.queryConfig = queryConfig;
        this.queryManager = queryManager;
    }

    static Map<String, String> getValidRequestTags(SourceType sourceType,
                                                   String serviceName,
                                                   String scriptName) {
        if ((SourceType.SERVICE.equals(sourceType) && Strings.isNullOrEmpty(serviceName)) || (
                SourceType.SCRIPT.equals(sourceType) && Strings.isNullOrEmpty(scriptName))) {
            throw new BadRequestException("Invalid source type and request tags", new IllegalArgumentException());
        }

        if (SourceType.SERVICE.equals(sourceType)) {
            return ImmutableMap.of(SourceType.SERVICE_NAME, serviceName);
        }

        if (SourceType.SCRIPT.equals(sourceType)) {
            return ImmutableMap.of(SourceType.SCRIPT_NAME, scriptName);
        }

        if (SourceType.FQL.equals(sourceType)) {
            return Maps.newHashMap();
        }

        throw new BadRequestException("Invalid source type and request tags", new IllegalArgumentException());

    }

    @GET
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download/{app_name}")
    @Timed
    @ApiOperation("runFqlGet")
    public StreamingOutput runFqlGet(@QueryParam("q") final String query,
                                     @HeaderParam(SOURCE_TYPE) final SourceType sourceType,
                                     @HeaderParam(SERVICE_NAME) final String serviceName,
                                     @HeaderParam(SCRIPT_NAME) final String scriptName) throws JsonProcessingException {
        Preconditions.checkNotNull(query);

        preprocess(query, sourceType);

        Map<String, String> requestTags = getValidRequestTags(sourceType, serviceName, scriptName);
        FqlRequest fqlRequest = new FqlRequest(query, false);
        final FlatRepresentation representation = fqlEngine.parse(fqlRequest, sourceType,
                requestTags);
        return output -> FlatToCsvConverter.convert(representation, new OutputStreamWriter(output));
    }

    @POST
    @Timed
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
    @Path("/extrapolation")
    @ApiOperation("runFqlPostWithExtrapolationFlag")
    public FlatRepresentation runFqlPostWithExtrapolationFlag(@HeaderParam(SOURCE_TYPE) @DefaultValue("FQL") final SourceType sourceType,
                                                              @HeaderParam(SERVICE_NAME) final String serviceName,
                                                              @HeaderParam(SCRIPT_NAME) final String scriptName,
                                                              final FqlRequest request)
            throws JsonProcessingException {
        preprocess(request.getQuery(), sourceType);
        return runFqlQuery(sourceType, serviceName, scriptName, request);
    }

    @POST
    @Timed
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
    @ApiOperation("runFqlPost")
    public FlatRepresentation runFqlPost(@HeaderParam(SOURCE_TYPE) final SourceType sourceType,
                                         @HeaderParam(SERVICE_NAME) final String serviceName,
                                         @HeaderParam(SCRIPT_NAME) final String scriptName,
                                         final String query) throws JsonProcessingException {
        preprocess(query, sourceType);

        FqlRequest request = new FqlRequest(query, false);

        return runFqlQuery(sourceType, serviceName, scriptName, request);
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/save")
    @ApiOperation("Save FQL query")
    public FqlStore saveFQL(final FqlStore fqlStore) {
        fqlStoreService.save(fqlStore);
        return fqlStore;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/get")
    @ApiOperation("Get List<FqlStore>")
    public List<FqlStore> get(FqlGetRequest fqlGetRequest) {
        if (queryConfig.isLogQueries()) {
            log.info("Fql Query : " + fqlGetRequest.toString());
        }
        return fqlStoreService.get(fqlGetRequest);
    }

    private FlatRepresentation runFqlQuery(SourceType sourceType,
                                           String serviceName,
                                           String scriptName,
                                           FqlRequest request) throws JsonProcessingException {
        Map<String, String> requestTags = getValidRequestTags(sourceType, serviceName, scriptName);
        return fqlEngine.parse(request, sourceType, requestTags);
    }

    private void preprocess(String query,
                            SourceType sourceType) {

        queryManager.checkIfQueryAllowed(query, sourceType);

        if (queryConfig.isLogQueries()) {
            if (query.contains("time")) {
                log.info("Fql Query : " + query);
            }
        }
    }
}
