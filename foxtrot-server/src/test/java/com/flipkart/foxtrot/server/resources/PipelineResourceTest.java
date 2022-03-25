package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.pipeline.PipelineManager;
import com.flipkart.foxtrot.core.pipeline.impl.FoxtrotPipelineManager;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PipelineResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    private PipelineManager pipelineManager;

    public PipelineResourceTest() throws Exception {
        super();
        this.pipelineManager = new FoxtrotPipelineManager(getPipelineMetadataManager());
        this.pipelineManager = spy(pipelineManager);
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new PipelineResource(pipelineManager))
                .build();
    }


    @Test
    public void testSaveNullPipeline() throws Exception {
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(null);
        assertEquals(422, response.getStatus());
    }

    @Test
    public void testSaveNullPipelineName() throws Exception {

        Pipeline pipeline = Pipeline.builder()
                .name(null)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(pipelineEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveBadPipelineName() throws Exception {

        Pipeline pipeline = Pipeline.builder()
                .name("adjk*^")
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(pipelineEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        doThrow(FoxtrotExceptions.createExecutionException("dummy", new IOException())).when(pipelineManager)
                .save(Matchers.<Pipeline>any());
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(pipelineEntity);
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        reset(pipelineManager);
    }

    @Test
    public void testSaveEmptyProcessors() throws Exception {
        reset(pipelineManager);
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of())
                .build();
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(pipelineEntity);
        assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }

    @Test
    public void testSaveHappyCase() throws Exception {
        reset(pipelineManager);
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME + "-A")
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        Response response = resources.target("/v1/pipeline")
                .request()
                .post(pipelineEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }


    private void compare(Pipeline expected,
                         Pipeline actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual Pipeline name should not be null", actual.getName());
        assertNotNull("Actual Pipeline Processors should not be null", actual.getProcessors());
        assertEquals("Actual Pipeline name should match expected Pipeline name", expected.getName(), actual.getName());
        assertTrue("Actual Pipeline processors should match expected Pipeline Processors", expected.getProcessors()
                .equals(actual.getProcessors()
                        .stream()
                        .map(x -> (StringLowerProcessorDefinition) x)
                        .collect(Collectors.toList())));
    }


    @Test
    public void testGetPipeline() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        Pipeline response = resources.target(String.format("/v1/pipeline/%s", TestUtils.TEST_PIPELINE_NAME))
                .request()
                .get(Pipeline.class);
        compare(pipeline, response);
    }

    @Test
    public void testGetAllPipeline() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();

        List<Pipeline> response = resources.target(String.format("/v1/pipeline", TestUtils.TEST_PIPELINE_NAME))
                .request()
                .get(new GenericType<List<Pipeline>>() {
                });
        assertEquals(1, response.size());
        compare(pipeline, response.get(0));
    }

    @Test
    public void testUpdatePipeline() throws Exception {
        Pipeline pipeline = Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build();
        pipeline.setProcessors(ImmutableList.of(new StringLowerProcessorDefinition("ds")));
        Entity<Pipeline> pipelineEntity = Entity.json(pipeline);
        Response response = resources.target(String.format("/v1/pipeline/%s", TestUtils.TEST_PIPELINE_NAME))
                .request()
                .put(pipelineEntity);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }
}
