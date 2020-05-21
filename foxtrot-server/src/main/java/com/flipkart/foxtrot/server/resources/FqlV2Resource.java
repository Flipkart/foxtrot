package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.gandalf.access.AccessService;
import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlGetRequest;
import com.flipkart.foxtrot.sql.fqlstore.FqlStore;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.io.OutputStreamWriter;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import lombok.extern.slf4j.Slf4j;

/***
 Created by mudit.g on Oct, 2019
 ***/
@Singleton
@Path("/v2/fql")
@Api(value = "/v2/fql")
@Slf4j
public class FqlV2Resource {

    private final QueryConfig queryConfig;
    private FqlEngine fqlEngine;
    private FqlStoreService fqlStoreService;
    private AccessService accessService;

    @Inject
    public FqlV2Resource(final FqlEngine fqlEngine, final FqlStoreService fqlStoreService, AccessService accessService,
            QueryConfig queryConfig) {
        this.fqlEngine = fqlEngine;
        this.fqlStoreService = fqlStoreService;
        this.accessService = accessService;
        this.queryConfig = queryConfig;
    }

    @GET
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download")
    @Timed
    @ApiOperation("runFqlGet")
    public StreamingOutput runFqlGet(@QueryParam("q") final String query) throws JsonProcessingException {
        Preconditions.checkNotNull(query);
        final FlatRepresentation representation = fqlEngine.parse(query, null, accessService);
        return output -> FlatToCsvConverter.convert(representation, new OutputStreamWriter(output));
    }

    @POST
    @Timed
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
    @ApiOperation("runFqlPost")
    public FlatRepresentation runFqlPost(final String query) throws JsonProcessingException {
        return fqlEngine.parse(query, null, accessService);
    }

    String getMessage(Throwable e) {
        Throwable root = e;
        while (null != root.getCause()) {
            root = root.getCause();
        }
        return root.getMessage();
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
}
