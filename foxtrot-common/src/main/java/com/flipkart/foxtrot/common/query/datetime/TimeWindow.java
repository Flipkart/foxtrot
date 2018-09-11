package com.flipkart.foxtrot.common.query.datetime;

import lombok.Builder;
import lombok.Data;

@Data
public class TimeWindow {
    private long startTime = 0L;
    private long endTime = 0L;

    public TimeWindow() {
    }

    @Builder
    public TimeWindow(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
