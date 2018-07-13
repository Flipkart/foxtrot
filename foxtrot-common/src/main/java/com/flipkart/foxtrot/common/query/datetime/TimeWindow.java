package com.flipkart.foxtrot.common.query.datetime;

import java.util.Date;

public class TimeWindow {
    private long startTime;
    private long endTime;

    public TimeWindow() {
    }

    public TimeWindow(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeWindow that = (TimeWindow) o;

        if (endTime != that.endTime) return false;
        if (startTime != that.startTime) return false;

        return true;
    }

    @Override
    public int hashCode() {
        long tmpStartTime = startTime / 30000;
        long tmpEndTime = endTime / 30000;
        int result = (int) (tmpStartTime ^ (tmpStartTime >>> 32));
        result = 31 * result + (int) (tmpEndTime ^ (tmpEndTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimeWindow{" +
                "startTime=" + new Date(startTime).toString() +
                ", endTime=" + new Date(endTime).toString() +
                '}';
    }
}
