package com.flipkart.foxtrot.common.histogram;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:14 AM
 */
public class HistogramResponse {
    public static class Count {
        private long period;
        private long count;

        public Count() {
        }

        public Count(long period, long count) {
            this.period = period;
            this.count = count;
        }

        public long getPeriod() {
            return period;
        }

        public void setPeriod(long period) {
            this.period = period;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
    private List<Count> counts;

    public HistogramResponse() {
    }

    public HistogramResponse(List<Count> counts) {
        this.counts = counts;
    }

    public List<Count> getCounts() {
        return counts;
    }

    public void setCounts(List<Count> counts) {
        this.counts = counts;
    }
}
