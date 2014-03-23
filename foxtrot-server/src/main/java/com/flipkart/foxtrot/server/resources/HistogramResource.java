package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 11:23 PM
 */
@Path("/foxtrot/v1/histogram")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class HistogramResource {
    private static final Logger logger = LoggerFactory.getLogger(HistogramResource.class.getSimpleName());

    private QueryStore queryStore;

    public HistogramResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @POST
    public Response runHistogram(@Valid final HistogramRequest histogramRequest) {
        try {
            return Response.ok(Collections.singletonMap("results", queryStore.histogram(histogramRequest))).build();
        } catch (Exception e) {
            logger.error("Error saving document: ", e);
            return Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build();
        }
    }
}
