package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.server.providers.FlatToCsvConverter;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.base.Preconditions;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStreamWriter;

@Path("/v1/fql")
public class FqlResource {
    private FqlEngine fqlEngine;

    public FqlResource(final FqlEngine fqlEngine) {
        this.fqlEngine = fqlEngine;
    }

    @GET
    @Produces(FoxtrotExtraMediaType.TEXT_CSV)
    @Path("/download")
    public StreamingOutput runFqlGet(@QueryParam("q") final String query) throws Exception {
        Preconditions.checkNotNull(query);
        final FlatRepresentation representation = fqlEngine.parse(query);
        return output -> FlatToCsvConverter.convert(representation, new OutputStreamWriter(output));
    }

    @POST
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
    public FlatRepresentation runFqlPost(final String query) throws Exception {
        return fqlEngine.parse(query);
    }

    String getMessage(Throwable e) {
        Throwable root = e;
        while (null != root.getCause()) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
