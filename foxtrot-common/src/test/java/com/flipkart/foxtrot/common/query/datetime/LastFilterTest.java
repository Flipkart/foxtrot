package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class LastFilterTest {
    private final ObjectMapper objectMapper;

    public LastFilterTest() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    @Test
    public void testGetWindow() throws Exception {
        final String json = "{\n" +
                "  \"operator\" : \"last\",\n" +
                "  \"duration\" : \"1h\"\n" +
                "}";
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertTrue((timeWindow.getEndTime() - timeWindow.getStartTime()) == 3600000);
    }

    @Test
    public void testGetWindowStart() throws Exception {
        final String json = "{\n" +
                "  \"operator\" : \"last\",\n" +
                "  \"currentTime\" : 10000,\n" +
                "  \"duration\" : \"1h\"\n" +
                "}";
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertEquals("_timestamp", lastFilter.getField());
        Assert.assertEquals(10000, lastFilter.getCurrentTime());
        Assert.assertTrue((timeWindow.getEndTime() - timeWindow.getStartTime()) == 3600000);
    }
}