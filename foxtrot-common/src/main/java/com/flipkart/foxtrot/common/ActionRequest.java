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
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.query.Filter;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:49 PM
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opcode")
public abstract class ActionRequest {

    private final String opcode;

    private String timeZone;

    private List<Filter> filters;

    protected ActionRequest(String opcode) {
        this.opcode = opcode;
        this.timeZone = "Asia/Kolkata";
    }

    protected ActionRequest(String opcode, List<Filter> filters) {
        this.opcode = opcode;
        this.filters = filters;
    }

    public String getOpcode() {
        return opcode;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }
}
