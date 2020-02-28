package com.flipkart.foxtrot.core.funnel.resources;

import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV1;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV2;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelEventResponseV2;
import com.flipkart.foxtrot.core.funnel.services.EventProcessingService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import java.util.stream.Collectors;
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
@Path("v2/events")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "v2/events")
public class EventProcessingResourceV2 {

    private EventProcessingService eventProcessingService;

    @Inject
    public EventProcessingResourceV2(EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @POST
    @Path("/process")
    @ApiOperation("Event Process Request")
    public EventProcessingResponseV2 processRequest(EventProcessingRequest eventProcessingRequest)
            throws FoxtrotException {
        return eventProcessingService.process(eventProcessingRequest);
    }
}
