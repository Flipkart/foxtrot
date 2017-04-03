package com.flipkart.foxtrot.common.query.datetime;

import io.dropwizard.util.Duration;
import org.joda.time.DateTime;

public class WindowUtil {

    private WindowUtil() {
    }

    public static TimeWindow calculate(Duration duration) {
        return calculate(System.currentTimeMillis(), duration);
    }

    public static TimeWindow calculate(long endTime, Duration duration) {
        DateTime windowStartTime = new DateTime(endTime - duration.toMilliseconds());
        switch (duration.getUnit()) {
            case NANOSECONDS:
            case MICROSECONDS:
            case MILLISECONDS:
            case SECONDS:
            case MINUTES:
                return new TimeWindow(windowStartTime.getMillis(), endTime);
            case HOURS:
                return new TimeWindow(nearestHourFloor(windowStartTime).getMillis(), endTime);
            case DAYS:
                return new TimeWindow(nearestDayFloor(windowStartTime).getMillis(), endTime);
            default:
                return new TimeWindow(windowStartTime.getMillis(), endTime);
        }
    }

    public static DateTime nearestHourFloor(DateTime dateTime) {
        return dateTime.hourOfDay().roundFloorCopy();
    }

    public static DateTime nearestDayFloor(DateTime dateTime) {
        return dateTime.dayOfYear().roundFloorCopy();
    }


}
