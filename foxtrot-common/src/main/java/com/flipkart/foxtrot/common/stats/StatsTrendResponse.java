package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;

import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendResponse implements ActionResponse {
    private List<BucketStats> result;

    public static class BucketStats {
        private Number period;
        private Map<String, Object> stats;

        public BucketStats() {

        }

        public BucketStats(Number period, Map<String, Object> stats) {
            this.period = period;
            this.stats = stats;
        }

        public Number getPeriod() {
            return period;
        }

        public void setPeriod(Number period) {
            this.period = period;
        }

        public Object getStats() {
            return stats;
        }

        public void setStats(Map<String, Object> stats) {
            this.stats = stats;
        }
    }

    public StatsTrendResponse() {

    }

    public StatsTrendResponse(List<BucketStats> result) {
        this.result = result;
    }

    public List<BucketStats> getResult() {
        return result;
    }

    public void setResult(List<BucketStats> result) {
        this.result = result;
    }
}
