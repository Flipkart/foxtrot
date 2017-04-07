package com.flipkart.foxtrot.common.query.datetime;

import io.dropwizard.util.Duration;
import org.joda.time.DateTime;

public class WindowUtil {

    private WindowUtil() {

    }

    public static TimeWindow calculate(long endTime, Duration duration, RoundingMode roundingMode) {
        DateTime windowStartTime = new DateTime(endTime - duration.toMilliseconds());
        if (roundingMode == null || roundingMode == RoundingMode.NONE) {
            return new TimeWindow(windowStartTime.getMillis(), endTime);
        }
        return new TimeWindow(roundingMode.translate(windowStartTime, duration).getMillis(), endTime);
    }
}
