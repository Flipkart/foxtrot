/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.common.trend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.enums.CountPrecision;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 2:30 PM
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class TrendRequest extends ActionRequest {

    private String table;

    private String field;

    private String timestamp = "_timestamp";

    private Period period = Period.days;

    private List<String> values;

    private String uniqueCountOn;

    private CountPrecision precision;

    private String consoleId;

    public TrendRequest() {
        super(Opcodes.TREND);
    }

    public TrendRequest(List<Filter> filters,
                        String table,
                        String field,
                        String timestamp,
                        Period period,
                        List<String> values,
                        String uniqueCountOn,
                        String consoleId,
                        boolean bypassCache,
                        Map<String, String> requestTags,
                        SourceType sourceType,
                        boolean extrapolationFlag) {
        super(Opcodes.TREND, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.table = table;
        this.field = field;
        this.timestamp = timestamp;
        this.period = period;
        this.values = values;
        this.uniqueCountOn = uniqueCountOn;
        this.consoleId = consoleId;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("timestamp", timestamp)
                .append("period", period)
                .append("values", values)
                .append("uniqueCountOn", uniqueCountOn)
                .append("consoleId", consoleId)
                .toString();
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
}
