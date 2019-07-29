/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.query.Filter;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:49 PM
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "opcode")
@NoArgsConstructor
public abstract class ActionRequest implements Serializable, Cloneable {

    private String opcode;

    private List<Filter> filters;

    protected ActionRequest(String opcode) {
        this.opcode = opcode;
    }

    protected ActionRequest(String opcode, List<Filter> filters) {
        this.opcode = opcode;
        this.filters = filters;
    }

    public String getOpcode() {
        return opcode;
    }

    public List<Filter> getFilters() {
        if (filters == null) {
            return Lists.newArrayList();
        }
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public abstract <T> T accept(ActionRequestVisitor<T> var1);

    public Object clone() throws CloneNotSupportedException {
        ActionRequest actionRequestClone = (ActionRequest) super.clone();
        actionRequestClone.setFilters(new ArrayList<>(this.filters));
        return actionRequestClone;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("opcode", opcode)
                .append("filters", filters)
                .toString();
    }

    public Object clone() throws CloneNotSupportedException {
        ActionRequest actionRequestClone = (ActionRequest) super.clone();
        List<Filter> filters = new ArrayList<>(this.filters);
        actionRequestClone.setFilters(filters);
        return actionRequestClone;
    }
}
