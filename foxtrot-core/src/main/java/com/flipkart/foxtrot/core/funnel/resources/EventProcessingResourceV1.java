package com.flipkart.foxtrot.core.funnel.resources;

import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV1;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV2;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelEventResponseV1;
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
@Path("/events")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/events")
public class EventProcessingResourceV1 {

    private EventProcessingService eventProcessingService;

    @Inject
    public EventProcessingResourceV1(EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @POST
    @Path("/process")
    @ApiOperation("Event Process Request")
    public EventProcessingResponseV1 processRequest(EventProcessingRequest eventProcessingRequest)
            throws FoxtrotException {
        EventProcessingResponseV2 responseV2 = eventProcessingService.process(eventProcessingRequest);

        if(CollectionUtils.isNullOrEmpty(responseV2.getFunnelEventResponses())) {
            return new EventProcessingResponseV1(responseV2.getResponseHashCode());
        }

        List<FunnelEventResponseV1> funnelEventResponses = responseV2.getFunnelEventResponses().stream()
                .map(funnelEventResponseV2 ->
                        FunnelEventResponseV1.builder()
                                .funnelInfos(funnelEventResponseV2.getFunnelInfos())
                                .identifierId(funnelEventResponseV2.getCategory())
                                .eventId(funnelEventResponseV2.getEventType())
                                .build()
                ).collect(Collectors.toList());
        return new EventProcessingResponseV1(funnelEventResponses);
    }
}
