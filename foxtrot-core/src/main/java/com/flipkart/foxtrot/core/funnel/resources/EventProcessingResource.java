package com.flipkart.foxtrot.core.funnel.resources;

import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponse;
import com.flipkart.foxtrot.core.funnel.services.EventProcessingService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Singleton
@Path("/events")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/events")
public class EventProcessingResource {

    private EventProcessingService eventProcessingService;

    @Inject
    public EventProcessingResource(EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @POST
    @Path("/process")
    @ApiOperation("Event Process Request")
    public EventProcessingResponse processRequest(EventProcessingRequest eventProcessingRequest) throws FoxtrotException {
        return eventProcessingService.process(eventProcessingRequest);
    }
}
