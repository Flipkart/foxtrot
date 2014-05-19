/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
