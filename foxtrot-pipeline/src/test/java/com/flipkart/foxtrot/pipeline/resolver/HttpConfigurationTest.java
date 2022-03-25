package com.flipkart.foxtrot.pipeline.resolver;

import lombok.val;
import org.junit.Test;

import static org.junit.Assert.*;

public class HttpConfigurationTest {

    @Test
    public void testValidation() {
        val underTest = new HttpConfiguration();
        assertEquals("foxtrot", underTest.getServiceName());
        assertFalse(underTest.isValidZkDiscoveryConfig());
        underTest.setNamespace("phonepe");
        underTest.setEnvironment("stage");
        assertTrue(underTest.isValidZkDiscoveryConfig());
        assertFalse(underTest.isValidEndpoint());
        underTest.setHost("foxtrot.example.org");
        underTest.setPort(80);
        assertTrue(underTest.isValidEndpoint());
        assertEquals("http://foxtrot.example.org:80", underTest.getUrl());
        underTest.setSecure(true);
        assertTrue(underTest.isValidEndpoint());
        assertEquals("https://foxtrot.example.org:80", underTest.getUrl());
    }
}