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
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import com.google.common.collect.Lists;
import io.dropwizard.validation.ValidationMethod;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:49 PM
 */
@Slf4j
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "opcode")
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ActionRequest implements Serializable, Cloneable {

    @Getter
    @Setter
    private String opcode;

    private List<Filter> filters;

    @Getter
    @Setter
    private boolean bypassCache;

    @Getter
    @Setter
    private Map<String, String> requestTags = new HashMap<>();

    @Getter
    @Setter
    @NotNull
    private SourceType sourceType;

    @Getter
    @Setter
    private boolean extrapolationFlag;

    protected ActionRequest(String opcode) {
        this.opcode = opcode;
    }

    protected ActionRequest(String opcode,
                            List<Filter> filters,
                            boolean bypassCache,
                            Map<String, String> requestTags,
                            SourceType sourceType,
                            boolean extrapolationFlag) {
        this.opcode = opcode;
        this.filters = filters;
        this.bypassCache = bypassCache;
        this.requestTags = requestTags;
        this.sourceType = sourceType;
        this.extrapolationFlag = extrapolationFlag;
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

    @JsonIgnore
    @ValidationMethod(message = "Invalid source type and request tags")
    public boolean isValid() {
        return sourceType.validate(requestTags);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("opcode", opcode)
                .append("filters", filters)
                .append("sourceType", sourceType)
                .append("requestTags", requestTags)
                .toString();
    }

}
