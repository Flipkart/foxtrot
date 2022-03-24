package com.flipkart.foxtrot.pipeline.processors.geo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.geo.GeoPoint;
import com.flipkart.foxtrot.pipeline.geo.GeoUtils;
import com.flipkart.foxtrot.pipeline.geo.PointExtractor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorExecutionException;
import com.flipkart.foxtrot.pipeline.processors.TargetWriteMode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.geometry.S2CellId;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

@Slf4j
public class AddS2GridProcessorTest {

    private AddS2GridProcessor s2GridProcessor;
    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        new PipelineModule();
        SerDe.init(mapper);
        s2GridProcessor = new AddS2GridProcessor(
                new AddS2GridProcessorDefinition("$.location", "$.target", ImmutableSet.of(5, 6),
                        TargetWriteMode.OVERWRITE), new PointExtractor());
    }

    @Test
    public void testS2GridProcessorHappyPath() throws ProcessorExecutionException {
        val doc1 = createDocument(ImmutableMap.of("location", ImmutableMap.of("lat", 12.77, "lon", 77.45)));
        s2GridProcessor.handle(doc1);
        assertFalse(doc1.getData()
                .at("/target/5")
                .isMissingNode());
        assertEquals(S2CellId.fromLatLng(GeoUtils.toS2LatLng(new GeoPoint(12.77, 77.45))).parent(5).toToken(), doc1.getData()
                .at("/target/5").asText());
        assertEquals(S2CellId.fromLatLng(GeoUtils.toS2LatLng(new GeoPoint(12.77, 77.45))).parent(6).toToken(), doc1.getData()
                .at("/target/6").asText());
    }

    @Test
    public void testS2GridProcessorMissingPath() {
        val doc1 = createDocument(ImmutableMap.of("locationAlt", ImmutableMap.of("lat", 12.77, "lon", 77.45)));
        try {
            s2GridProcessor.handle(doc1);
        } catch (ProcessorExecutionException e) {
            fail("Processor should ignore missing Paths");
        }
    }

    @Test
    public void testS2GridProcessorTargetPathExistsWithOverwrite() {
        val s2GridProcessor2 = new AddS2GridProcessor(
                new AddS2GridProcessorDefinition("$.location", "$.target", ImmutableSet.of(5),
                        TargetWriteMode.OVERWRITE), new PointExtractor());
        val doc1 = createDocument(ImmutableMap.of("location", ImmutableMap.of("lat", 12.77, "lon", 77.45), "tager",
                ImmutableMap.of("5", "GRID")));
        s2GridProcessor2.handle(doc1);
        assertNotNull(doc1.getData()
                .at("/target/5")
                .asText());
    }

    @Test
    public void testS2GridProcessorTargetPathExistsWithCreateOnly() {
        val s2GridProcessor2 = new AddS2GridProcessor(
                new AddS2GridProcessorDefinition("$.location", "$.target", ImmutableSet.of(5),
                        TargetWriteMode.CREATE_ONLY), new PointExtractor());
        val doc1 = createDocument(ImmutableMap.of("location", ImmutableMap.of("lat", 12.77, "lon", 77.45), "target",
                ImmutableMap.of("5", "GRID")));
        try {
            s2GridProcessor2.handle(doc1);
            fail("Exception should have been raised");
        } catch (ProcessorExecutionException e) {

        }
    }

    @SneakyThrows
    private Document createDocument(Map<String, Object> of) {
        val doc = new Document();
        doc.setData(mapper.readTree(mapper.writeValueAsString(of)));
        return doc;
    }
}