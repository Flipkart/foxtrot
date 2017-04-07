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
        Assert.assertTrue((timeWindow.getEndTime() - timeWindow.getStartTime()) == 3600000);
    }

    @Test
    public void testGetWindowStartFloor() throws Exception {
        DateTime currentTime = new DateTime();
        final String json = String.format("{ \"operator\": \"last\", \"currentTime\": %d, \"roundingMode\": \"FLOOR\", \"duration\": \"1h\" }",
                currentTime.getMillis());
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertEquals("_timestamp", lastFilter.getField());
        Assert.assertEquals(currentTime.getMillis(), lastFilter.getCurrentTime());
        Assert.assertEquals(currentTime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).minusHours(1).getMillis(),
                timeWindow.getStartTime());
    }

    @Test
    public void testGetWindowStartCeiling() throws Exception {
        DateTime currentTime = new DateTime();
        final String json = String.format("{ \"operator\": \"last\", \"currentTime\": %d, \"roundingMode\": \"CEILING\", \"duration\": \"1h\" }",
                currentTime.getMillis());
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertEquals("_timestamp", lastFilter.getField());
        Assert.assertEquals(currentTime.getMillis(), lastFilter.getCurrentTime());
        Assert.assertEquals(currentTime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).getMillis(),
                timeWindow.getStartTime());
    }

    @Test
    public void testGetWindowStartNoRounding() throws Exception {
        DateTime currentTime = new DateTime();
        final String json = String.format("{ \"operator\": \"last\", \"currentTime\": %d, \"duration\": \"1h\" }",
                currentTime.getMillis());
        LastFilter lastFilter = objectMapper.readValue(json, LastFilter.class);
        TimeWindow timeWindow = lastFilter.getWindow();
        Assert.assertEquals("_timestamp", lastFilter.getField());
        Assert.assertEquals(currentTime.getMillis(), lastFilter.getCurrentTime());
        Assert.assertEquals(currentTime.minusHours(1).getMillis(), timeWindow.getStartTime());
    }

}