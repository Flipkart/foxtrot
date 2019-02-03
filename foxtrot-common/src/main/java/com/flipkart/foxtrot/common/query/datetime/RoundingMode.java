package com.flipkart.foxtrot.common.query.datetime;

import io.dropwizard.util.Duration;
import org.joda.time.DateTime;


public enum RoundingMode {

    FLOOR {
        @Override
        public DateTime translate(DateTime dateTime, Duration duration) {
            switch (duration.getUnit()) {
                case NANOSECONDS:
                case MICROSECONDS:
                    return dateTime;
                case MILLISECONDS:
                    return dateTime.millisOfSecond().roundFloorCopy();
                case SECONDS:
                    return dateTime.secondOfMinute().roundFloorCopy();
                case MINUTES:
                    return dateTime.minuteOfHour().roundFloorCopy();
                case HOURS:
                    return dateTime.hourOfDay().roundFloorCopy();
                case DAYS:
                    return dateTime.dayOfMonth().roundFloorCopy();
                default:
                    return dateTime;
            }
        }
    },
    CEILING {
        @Override
        public DateTime translate(DateTime dateTime, Duration duration) {
            switch (duration.getUnit()) {
                case NANOSECONDS:
                case MICROSECONDS:
                    return dateTime;
                case MILLISECONDS:
                    return dateTime.millisOfSecond().roundCeilingCopy();
                case SECONDS:
                    return dateTime.secondOfMinute().roundCeilingCopy();
                case MINUTES:
                    return dateTime.minuteOfHour().roundCeilingCopy();
                case HOURS:
                    return dateTime.hourOfDay().roundCeilingCopy();
                case DAYS:
                    return dateTime.dayOfMonth().roundCeilingCopy();
                default:
                    return dateTime;
            }
        }
    },
    NONE {
        @Override
        public DateTime translate(DateTime dateTime, Duration duration) {
            return dateTime;
        }
    };

    public abstract DateTime translate(DateTime dateTime, Duration duration);

}
