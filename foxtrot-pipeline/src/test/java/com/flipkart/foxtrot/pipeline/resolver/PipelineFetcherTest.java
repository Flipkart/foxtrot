package com.flipkart.foxtrot.pipeline.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.PipelineExecutionMode;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public abstract class PipelineFetcherTest {
    public static final String PIPELINE_NAME = "PIPELINE";
    public static final Pipeline pipeline = Pipeline.builder()
            .name(PIPELINE_NAME)
            .processors(ImmutableList.of(new StringLowerProcessorDefinition("$.matchField")))
            .executionMode(PipelineExecutionMode.SERIAL)
            .ignoreErrors(true)
            .build();
    public static final ObjectMapper objectMapper = new ObjectMapper();

    abstract protected PipelineFetcher underTest() throws Exception;

    @Before
    public void setUp() throws Exception {
        SerDe.init(objectMapper);
        PipelineUtils.init(objectMapper, ImmutableSet.of("com.flipkart.foxtrot.pipeline.processors"));
    }

    @Test
    public void testFetchExistingPipeline() throws Exception {
        val pipeline = underTest().fetch(PIPELINE_NAME);
        assertNotNull(pipeline);
        assertEquals(PIPELINE_NAME, pipeline.getName());
        assertEquals(1, pipeline.getProcessors().size());
        assertEquals("STR::LOWER", pipeline.getProcessors().get(0).getType());
        assertEquals(PipelineExecutionMode.SERIAL, pipeline.getExecutionMode());
        assertTrue(pipeline.isIgnoreErrors());
    }

    @Test
    public void testFetchNonExistingPipeline() throws Exception {
        val pipeline = underTest().fetch("NON_EXISTENT_PIPELINE");
        assertNull(pipeline);
    }
}