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

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 2:30 PM
 */
public class TrendRequest extends ActionRequest {

    private String table;

    private String field;

    private String timestamp = "_timestamp";

    private Period period = Period.days;

    private List<String> values;

    private String uniqueCountOn;

    public TrendRequest() {
        super(Opcodes.TREND);
    }

    public TrendRequest(List<Filter> filters, String table, String field, String timestamp, Period period, List<String> values, String uniqueCountOn) {
        super(Opcodes.TREND, filters);
        this.table = table;
        this.field = field;
        this.timestamp = timestamp;
        this.period = period;
        this.values = values;
        this.uniqueCountOn = uniqueCountOn;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getUniqueCountOn() {
        return uniqueCountOn;
    }

    public void setUniqueCountOn(String uniqueCountOn) {
        this.uniqueCountOn = uniqueCountOn;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("timestamp", timestamp)
                .append("period", period)
                .append("values", values)
                .append("uniqueCountOn", uniqueCountOn)
                .toString();
    }
}
