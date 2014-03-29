package com.flipkart.foxtrot.common.histogram;

import com.flipkart.foxtrot.common.ActionResponse;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:14 AM
 */
public class HistogramResponse implements ActionResponse {
    public static class Count {
        private Number period;
        private long count;

        public Count() {
        }

        public Count(Number period, long count) {
            this.period = period;
            this.count = count;
        }

        public Number getPeriod() {
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
