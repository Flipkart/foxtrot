package com.flipkart.foxtrot.pipeline.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;
import lombok.val;

import java.io.IOException;

public class PointExtractorTest extends TestCase {

    public void testPointExtraction() throws IOException {
        val underTest = new PointExtractor();
        val mapper = new ObjectMapper();
        val testCases = ImmutableList.of(
                "{\"location\": {\"lat\": 1, \"lng\":2}}",
                "{\"location\": {\"latitude\": 1, \"longitude\":2}}",
                "{\"location\": {\"lat\": 1, \"lon\":2}}",
                "{\"location\": [2, 1]}");
        for (String test : testCases) {
            val jsonNode = mapper.readTree(test);
            JsonNode targetNode = jsonNode.get("location");
            val point = underTest.extractFromRoot(targetNode);
            assertTrue("Didnt Extract Point in " + test, point.isPresent());
            assertEquals(2.0, point.get()
                    .getLng());
            assertEquals(1.0, point.get()
                    .getLat());
        }

        val failureTestCase = ImmutableList.of("{\"location\": {\"lat\": \"1\", \"lng\":2}}",
                "{\"location\": {\"lat\": 1, \"lon\":\"3\"}}", "{\"location\": [2, 1, 3]}",
                "{\"location\": [2, \"1\"]}", "{\"location\": [\"2\", 1]}", "{}");
        for (String test : failureTestCase) {
            val jsonNode = mapper.readTree(test);
            JsonNode targetNode = jsonNode.get("location");
            val point = underTest.extractFromRoot(targetNode);
            assertFalse("Did Extract Point in " + test, point.isPresent());

        }

    }
}