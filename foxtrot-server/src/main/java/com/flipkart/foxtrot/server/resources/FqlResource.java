package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.access.AccessService;
import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlGetRequest;
import com.flipkart.foxtrot.sql.fqlstore.FqlStore;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.base.Preconditions;
import com.phonepe.gandalf.client.annotation.GandalfUserContext;
import com.phonepe.gandalf.models.user.UserDetails;
import io.dropwizard.primer.auth.annotation.Authorize;
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

@Path("/v1/fql")
@Api(value = "/v1/fql")
public class FqlResource {

    private FqlEngine fqlEngine;
    private FqlStoreService fqlStoreService;
    private AccessService accessService;

    public FqlResource(final FqlEngine fqlEngine, final FqlStoreService fqlStoreService, AccessService accessService) {
        this.fqlEngine = fqlEngine;
        this.fqlStoreService = fqlStoreService;
        this.accessService = accessService;
    }

    @GET
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download")
    @Timed
    @ApiOperation("runFqlGet")
    @Authorize(value = {})
    public StreamingOutput runFqlGet(@QueryParam("q") final String query, @GandalfUserContext UserDetails userDetails)
            throws JsonProcessingException {
        Preconditions.checkNotNull(query);
        final FlatRepresentation representation = fqlEngine.parse(query, userDetails, accessService);
        return output -> FlatToCsvConverter.convert(representation, new OutputStreamWriter(output));
    }

    @POST
    @Timed
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
    @ApiOperation("runFqlPost")
    @Authorize(value = {})
    public FlatRepresentation runFqlPost(final String query, @GandalfUserContext UserDetails userDetails)
            throws JsonProcessingException {
        return fqlEngine.parse(query, userDetails, accessService);
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
        return fqlStoreService.get(fqlGetRequest);
    }
}
