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
package com.flipkart.foxtrot.common.histogram;

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
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:06 AM
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class HistogramRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    @NotNull
    @NotEmpty
    private String field;

    @NotNull
    private Period period;

    private CountPrecision precision;

    private String consoleId;

    private String uniqueCountOn;

    public HistogramRequest() {
        super(Opcodes.HISTOGRAM);
        this.field = "_timestamp";
        this.period = Period.minutes;
    }

    public HistogramRequest(List<Filter> filters,
                            String table,
                            String field,
                            String uniqueCountOn,
                            Period period,
                            String consoleId,
                            boolean bypassCache,
                            Map<String, String> requestTags,
                            SourceType sourceType,
                            boolean extrapolationFlag) {
        super(Opcodes.HISTOGRAM, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.table = table;
        this.field = field;
        this.uniqueCountOn = uniqueCountOn;
        this.period = period;
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
                .append("uniqueCountOn", uniqueCountOn)
                .append("period", period)
                .append("consoleId", consoleId)
                .toString();
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public String getUniqueCountOn() {
        return uniqueCountOn;
    }

    public void setUniqueCountOn(String uniqueCountOn) {
        this.uniqueCountOn = uniqueCountOn;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
