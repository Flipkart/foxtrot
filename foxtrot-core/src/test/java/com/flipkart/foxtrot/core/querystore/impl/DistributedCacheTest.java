package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class DistributedCacheTest {
    private static final Logger logger = LoggerFactory.getLogger(DistributedCacheTest.class.getSimpleName());
    private static DistributedCache distributedCache;
    private static HazelcastInstance hazelcastInstance;
    private static ObjectMapper mapper;
    private static JsonNodeFactory factory;

    @BeforeClass
    public static void setUp() throws Exception {
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        mapper = new ObjectMapper();
        mapper.registerSubtypes(GroupResponse.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        distributedCache = new DistributedCache(hazelcastConnection, "TEST", mapper);
        factory = JsonNodeFactory.instance;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hazelcastInstance.shutdown();
    }

    @Test
    public void testPut() throws Exception {
        logger.info("Testing Distributed Cache - PUT");
        ActionResponse actionResponse = new GroupResponse(Collections.<String, Object>singletonMap("Hello", "world"));
        ActionResponse returnResponse = distributedCache.put("DUMMY_KEY_PUT", actionResponse);
        assertEquals(actionResponse, returnResponse);
        ObjectNode resultNode = factory.objectNode();
        resultNode.put("opcode", "GroupResponse");
        resultNode.put("result", factory.objectNode().put("Hello", "world"));
        String expectedResponse = mapper.writeValueAsString(resultNode);
        String actualResponse = hazelcastInstance.getMap("TEST").get("DUMMY_KEY_PUT").toString();
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Distributed Cache - PUT");
    }

    @Test
    public void testGet() throws Exception {
        logger.info("Testing Distributed Cache - GET");
        GroupResponse baseRequest = new GroupResponse();
        baseRequest.setResult(Collections.<String, Object>singletonMap("Hello", "World"));
        String requestString = mapper.writeValueAsString(baseRequest);
        hazelcastInstance.getMap("TEST").put("DUMMY_KEY_GET", requestString);
        ActionResponse actionResponse = distributedCache.get("DUMMY_KEY_GET");
        String actualResponse = mapper.writeValueAsString(actionResponse);
        assertEquals(requestString, actualResponse);
        logger.info("Tested Distributed Cache - GET");
    }

    @Test(expected = NullPointerException.class)
    public void testGetMissing() throws Exception {
        logger.info("Testing Distributed Cache - GET - Missing Key");
        distributedCache.get("DUMMY_KEY_MISSING");
        logger.info("Tested Distributed Cache - GET - Missing Key");
    }

    @Test
    public void testHas() throws Exception {
        logger.info("Testing Distributed Cache - HAS");
        hazelcastInstance.getMap("TEST").put("DUMMY_KEY_HAS", Collections.singletonMap("Hello", "world"));
        boolean response = distributedCache.has("DUMMY_KEY_HAS");
        assertTrue(response);
        response = distributedCache.has("INVALID_KEY");
        assertFalse(response);
        response = distributedCache.has(null);
        assertFalse(response);
        logger.info("Tested Distributed Cache - HAS");
    }
}
