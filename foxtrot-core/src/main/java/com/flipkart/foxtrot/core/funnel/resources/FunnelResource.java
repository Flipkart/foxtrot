package com.flipkart.foxtrot.core.funnel.resources;

import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import com.flipkart.foxtrot.core.funnel.services.FunnelService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Singleton
@Path("/funnel")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/funnel")
public class FunnelResource {

    private final FunnelService funnelService;

    @Inject
    public FunnelResource(final FunnelService funnelService) {
        this.funnelService = funnelService;
    }

    /**
     * Create Funnel marked waiting for approval
     *
     * @param funnel {@link Funnel}
     */
    @POST
    @ApiOperation("Create Funnel")
    public Response saveFunnel(Funnel funnel) {
        Funnel savedFunnel = funnelService.save(funnel);
        return Response.status(Status.CREATED)
                .entity(savedFunnel)
                .build();
    }

    /**
     * Update Funnel, only WAITING_FOR_APPROVAL Funnel can be updated
     */
    @PUT
    @Path("/{documentId}")
    @ApiOperation("Update Funnel via document id")
    public Response updateFunnel(@PathParam("documentId") final String documentId,
                                 Funnel funnel) {
        Funnel updatedFunnel = funnelService.update(documentId, funnel);
        return Response.ok(updatedFunnel)
                .build();
    }

    /**
     * Approve Funnel, move status to APPROVED
     */
    @PUT
    @Path("/approve/{documentId}")
    @ApiOperation("Approve Funnel via document id")
    public Response approveFunnel(@PathParam("documentId") final String documentId) {

        Funnel approvedFunnel = funnelService.approve(documentId);
        return Response.ok(approvedFunnel)
                .build();
    }


    /**
     * Reject Funnel, move status to REJECTED and soft delete
     */
    @PUT
    @Path("/reject/{documentId}")
    @ApiOperation("Reject Funnel via document id")
    public Response rejectFunnel(@PathParam("documentId") final String documentId) {
        Funnel rejectedFunnel = funnelService.reject(documentId);
        return Response.ok(rejectedFunnel)
                .build();
    }

    @GET
    @Path("/{id}")
    @ApiOperation("Get Funnel via funnel id")
    public Response getFunnelByFunnelId(@PathParam("id") final String funnelId) {
        Funnel funnel = funnelService.getFunnelByFunnelId(funnelId);
        return Response.ok(funnel)
                .build();
    }

    @GET
    @Path("/document/{documentId}")
    @ApiOperation("Get Funnel via document id")
    public Response getFunnelByDocumentId(@PathParam("documentId") final String documentId) {
        Funnel funnel = funnelService.getFunnelByDocumentId(documentId);
        return Response.ok(funnel)
                .build();
    }

    @GET
    @Path("/get")
    @ApiOperation("Get All Funnel Request")
    public Response getAll(@QueryParam("deleted") @DefaultValue("false") boolean deleted) {
        List<Funnel> funnels = funnelService.getAll(deleted);
        return Response.ok(funnels)
                .build();
    }

    @DELETE
    @Path("/{id}")
    @ApiOperation("Delete Funnel via funnel id")
    public Response deleteFunnel(@PathParam("id") final String funnelId) {
        funnelService.delete(funnelId);
        return Response.status(Status.ACCEPTED)
                .build();
    }

    @POST
    @Path("/search")
    @ApiOperation("Search Funnel")
    public Response searchFunnel(FilterRequest filterRequest) {
        FunnelFilterResponse funnelFilterResponse = funnelService.searchFunnel(filterRequest);
        return Response.ok(funnelFilterResponse)
                .build();
    }

    @GET
    @Path("/dropdown")
    @ApiOperation("Get dropdown values")
    public Response getDropdownValues() {
        return Response.ok(funnelService.getDropdownValues())
                .build();
    }

}
