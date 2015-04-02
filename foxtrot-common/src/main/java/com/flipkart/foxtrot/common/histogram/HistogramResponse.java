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
package com.flipkart.foxtrot.common.histogram;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:14 AM
 */
public class HistogramResponse implements ActionResponse {
    private long from;
    private long to;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Count)) return false;

            Count count1 = (Count) o;

            if (count != count1.count) return false;
            if (!period.equals(count1.period)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = period.hashCode();
            result = 31 * result + (int) (count ^ (count >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Count{" +
                    "period=" + period +
                    ", count=" + count +
                    '}';
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


    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }
    
}
