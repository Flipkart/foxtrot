package com.flipkart.foxtrot.common.query.datetime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
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
        Assert.assertTrue((timeWindow.getStartTime() == new DateTime().minusHours(1).hourOfDay().roundFloorCopy().getMillis()));
    }

    @Test
    public void testGetWindowStart() throws Exception {
        final String json = "{ \"operator\" : \"last\", \"currentTime\" : 1485842573572, \"duration\" : \"1h\" }";
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertEquals("_timestamp", lastFilter.getField());
        Assert.assertEquals(1485842573572L, lastFilter.getCurrentTime());
        Assert.assertTrue(timeWindow.getStartTime() == 1485837000000L);
    }
}