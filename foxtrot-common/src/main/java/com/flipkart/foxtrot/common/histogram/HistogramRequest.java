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

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 12:06 AM
 */
public class HistogramRequest extends ActionRequest {

    private String table;

    private String field = "_timestamp";

    private Period period;

    public HistogramRequest() {
        this.field = "_timestamp";
        this.period = Period.minutes;
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

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("filters", getFilters())
                .append("field", field)
                .append("period", period)
                .toString();
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = new HashSet<>();
        if (CollectionUtils.isStringNullOrEmpty(table)) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isStringNullOrEmpty(field)) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if (period == null) {
            validationErrors.add("time period cannot be null");
        }
        return validationErrors;
    }
}
