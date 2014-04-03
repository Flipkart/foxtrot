package com.flipkart.foxtrot.common.trend;

import com.flipkart.foxtrot.common.ActionResponse;

import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 2:31 PM
 */
public class TrendResponse implements ActionResponse {

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

    private Map<String, List<Count>> trends;

    public TrendResponse() {
    }

    public TrendResponse(Map<String, List<Count>> trends) {

        this.trends = trends;
    }

    public Map<String, List<Count>> getTrends() {
        return trends;
    }

    public void setTrends(Map<String, List<Count>> trends) {
        this.trends = trends;
    }

}
