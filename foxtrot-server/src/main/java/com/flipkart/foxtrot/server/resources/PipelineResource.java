package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.core.pipeline.PipelineManager;
import com.flipkart.foxtrot.pipeline.Pipeline;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Slf4j
@Path("/v1/pipeline")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/pipeline")
@Singleton
public class PipelineResource {

    private final PipelineManager pipelineManager;

    @Inject
    public PipelineResource(PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
    }

    @GET
    @Timed
    @Path("/{name}")
    @ApiOperation("Get pipeline")
    public Response get(@NotEmpty @PathParam("name") String name) {
        Pipeline pipeline = pipelineManager.get(name);
        return Response.ok()
                .entity(pipeline)
                .build();
    }

    @GET
    @Timed
    @ApiOperation("Get all pipeline")
    public Response getAll() {
        return Response.ok()
                .entity(pipelineManager.getAll())
                .build();
    }

    @POST
    @Timed
    @ApiOperation("Save pipeline")
    public Response save(@NotNull @Valid final Pipeline pipeline) {
        pipelineManager.save(pipeline);
        return Response.ok(pipeline)
                .build();
    }

    @PUT
    @Timed
    @Path("/{pipelineName}")
    @ApiOperation("Update Pipeline")
    public Response update(@PathParam("pipelineName") final String pipelineName,
                           @NotNull @Valid final Pipeline pipeline) {
        pipeline.setName(pipelineName);
        pipelineManager.update(pipeline);
        return Response.ok()
                .build();
    }

}
