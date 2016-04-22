package com.flipkart.foxtrot.common.query.datetime;

import io.dropwizard.util.Duration;

public class WindowUtil {

    private WindowUtil() {
    }

    public static TimeWindow calculate(Duration duration) {
        return calculate(System.currentTimeMillis(), duration);
    }

    public static TimeWindow calculate(long endTime, Duration duration) {
        long windowStartTime = endTime - duration.toMilliseconds();
        return new TimeWindow(windowStartTime, endTime);
    }

}
