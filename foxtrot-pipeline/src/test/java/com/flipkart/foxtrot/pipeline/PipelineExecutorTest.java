package com.flipkart.foxtrot.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.ProcessorInitializationException;
import com.flipkart.foxtrot.pipeline.processors.factory.CachedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.factory.ProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.geo.RegionMatchProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import junit.framework.TestCase;
import lombok.val;
import org.junit.Test;

public class PipelineExecutorTest extends TestCase {

    private PipelineExecutor executor;
    private ObjectMapper mapper;

    @Override
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        PipelineUtils.init(mapper, ImmutableSet.of("com.flipkart.foxtrot.pipeline"));

        Injector injector = Guice.createInjector(new PipelineModule(), new AbstractModule() {
            @Override
            protected void configure() {
                super.configure();
            }

            @Provides
            public PipelineConfiguration providesPC() {
                return new PipelineConfiguration();
            }
        });
        executor = new PipelineExecutor(new ReflectionBasedProcessorFactory(injector));
    }


    @Test
    public void testProcessorDeserialization() throws Exception {
        val pipelineString = "{\"name\":\"PIPELINE_NAME\",\"processors\":[{\"type\":\"GEO::REGION_MATCHER\",\"matchField\":\"location\",\"targetFieldRootMapping\":{\"$.target.state\":\"name\"}}]}";
        val pipeline = mapper.readValue(pipelineString, Pipeline.class);
        assertEquals("PIPELINE_NAME", pipeline.getName());
        assertEquals(1, pipeline.getProcessors()
                .size());
        assertTrue(pipeline.getProcessors()
                .iterator()
                .next() instanceof RegionMatchProcessorDefinition);
    }

    @Test
    public void testRegionEnrichmentExecute() throws Exception {
        val pipeline = Pipeline.builder()
                .name("PIPELINE_NAME")
                .processors(ImmutableList.of(RegionMatchProcessorDefinition.builder()
                        .matchField("location")
                        .targetFieldRootMapping(ImmutableMap.of("$.target.state", "name"))
                        .geoJsonSource("res://states-geojson.json")
                        .build()))
                .build();

        val documentString = "{\"location\":{\"lat\":12.79,\"lon\":78.47},  \"tenantName\": \"GUARDIAN\"}";
        val document = new Document("ID", 1, mapper.readTree(documentString));
        executor.execute(pipeline, document);
        assertEquals("Andhra Pradesh", document.getData()
                .get("target")
                .get("state")
                .asText());
    }

    @Test
    public void testLowerExecute() throws Exception {
        val pipeline = Pipeline.builder()
                .name("NAME")
                .processors(ImmutableList.of(StringLowerProcessorDefinition.builder()
                        .matchField("$.tenantName")
                        .build()))
                .build();

        val documentString = "{\"location\":{\"lat\":12.79,\"lon\":78.47},  \"tenantName\": \"GUARDIAN\"}";
        val document = new Document("ID", 1, mapper.readTree(documentString));
        executor.execute(pipeline, document);
        assertEquals("guardian", document.getData()
                .get("tenantName")
                .asText());
    }

    @Test
    public void testPipelineErrorHandling() throws Exception {
        val pipeline1 = Pipeline.builder()
                .name("NAME")
                .processors(ImmutableList.of(StringLowerProcessorDefinition.builder()
                        .matchField("$.tenant.Name")
                        .build()))
                .build();

        val documentString = "{\"location\":{\"lat\":12.79,\"lon\":78.47},  \"tenantName\": \"GUARDIAN\"}";
        val document = new Document("ID", 1, mapper.readTree(documentString));
        try {
            executor.execute(pipeline1, document);
            fail("Exception should have been raised");
        } catch (Exception e) {
        }
        pipeline1.setIgnoreErrors(true);
        try {
            executor.execute(pipeline1, document);
        } catch (Exception e) {
            fail("Exception should not have been raised");
        }

    }


    @Test
    public void testPipelineProcessorInitializationExceptionHandle() throws Exception {
        val localRegistry = new CachedProcessorFactory(new ProcessorFactory() {
            @Override
            public Processor create(ProcessorDefinition process) throws ProcessorCreationException {
                return new Processor() {
                    @Override
                    public void setProcessorDefinition(ProcessorDefinition processor) {

                    }

                    @Override
                    public void init() throws ProcessorInitializationException {
                        throw new ProcessorInitializationException("Testing exception", null);
                    }

                    @Override
                    public void handle(Document document) {
                        fail("Handle cant be called once initialization fails");
                    }
                };
            }
        }, CacheBuilder.newBuilder());
        val exec = new PipelineExecutor(localRegistry);
        val pipeline1 = Pipeline.builder()
                .name("NAME")
                .processors(ImmutableList.of(StringLowerProcessorDefinition.builder()
                        .matchField("$.tenant.Name")
                        .build()))
                .ignoreErrors(true)
                .build();

        val documentString = "{\"location\":{\"lat\":12.79,\"lon\":78.47},  \"tenantName\": \"GUARDIAN\"}";
        val document = new Document("ID", 1, mapper.readTree(documentString));
        try {
            exec.execute(pipeline1, document);
        } catch (Exception e) {
            fail("Exception should not have been raised");
        }
        pipeline1.setIgnoreErrors(false);
        try {
            exec.execute(pipeline1, document);
            fail("Exception should  have been raised");

        } catch (Exception e) {
        }

    }
}