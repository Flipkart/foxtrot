package com.flipkart.foxtrot.core.pipeline.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.core.pipeline.PipelineManager;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.pipeline.processor.TestProcessorDefinition;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class FoxtrotPipelineManagerTest {

    public static final ProcessorDefinition TEST_PROCESSOR_DEFINITION = new TestProcessorDefinition("$.stringField");
    private PipelineManager pipelineManager;
    private PipelineMetadataManager metadataManager;
    private QueryStore queryStore;

    @Before
    public void setUp() throws Exception {
        this.queryStore = mock(QueryStore.class);
        this.metadataManager = mock(PipelineMetadataManager.class);
        this.pipelineManager = new FoxtrotPipelineManager(metadataManager);
    }

    @Test
    public void savePipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        Pipeline pipeline = new Pipeline();
        pipeline.setName("PIPELINE");
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        pipelineManager.save(pipeline);
        assertTrue(true);
    }

    @Test
    public void saveExistingPipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        doNothing().when(queryStore)
                .initializeTable(any(Table.class));
        doReturn(true).when(metadataManager)
                .exists(any(String.class));
        try {
            Pipeline pipeline = new Pipeline();
            pipeline.setName("PIPELINE");
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            pipelineManager.save(pipeline);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.PIPELINE_ALREADY_EXISTS, e.getCode());
        }
    }

    @Test
    public void updatePipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        doReturn(true).when(metadataManager)
                .exists(anyString());
        Pipeline pipeline = new Pipeline();
        pipeline.setName("PIPELINE");
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        pipelineManager.update(pipeline);
        assertTrue(true);
    }

    @Test
    public void updateNonExistingPipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        doReturn(false).when(metadataManager)
                .exists(anyString());

        try {
            Pipeline pipeline = new Pipeline();
            pipeline.setName("PIPELINE");
            pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
            pipelineManager.update(pipeline);
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.PIPELINE_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void getPipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        Pipeline pipeline = new Pipeline();
        pipeline.setName("pipeline");
        pipeline.setProcessors(ImmutableList.of(TEST_PROCESSOR_DEFINITION));
        doReturn(pipeline).when(metadataManager)
                .get("pipeline");
        Pipeline getPipeline = pipelineManager.get("pipeline");
        assertEquals("pipeline", getPipeline.getName());
        assertEquals(1, getPipeline.getProcessors()
                .size());
        assertTrue(CollectionUtils.isEqualCollection(ImmutableList.of(TEST_PROCESSOR_DEFINITION),
                getPipeline.getProcessors()));
    }

    @Test
    public void getNullPipeline() throws Exception {
        doNothing().when(metadataManager)
                .save(any(Pipeline.class));
        doReturn(null).when(metadataManager)
                .get("pipeline");
        try {
            Pipeline getPipeline = pipelineManager.get("pipeline");
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.PIPELINE_NOT_FOUND, e.getCode());
        }
    }
}