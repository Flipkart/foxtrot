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
package com.flipkart.foxtrot.common.group;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.enums.CountPrecision;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.stats.Stat;
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
 * Time: 4:52 PM
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String uniqueCountOn;

    private String aggregationField;

    private Stat aggregationType;

    @NotNull
    @NotEmpty
    private List<String> nesting;

    private String consoleId;

    private CountPrecision precision;

    public GroupRequest() {
        super(Opcodes.GROUP);
    }

    public GroupRequest(List<Filter> filters,
                        String table,
                        String uniqueCountOn,
                        String aggregationField,
                        Stat aggregationType,
                        List<String> nesting,
                        String consoleId,
                        CountPrecision precision,
                        boolean bypassCache,
                        Map<String, String> requestTags,
                        SourceType sourceType,
                        boolean extrapolationFlag) {
        super(Opcodes.GROUP, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.table = table;
        this.uniqueCountOn = uniqueCountOn;
        this.aggregationField = aggregationField;
        this.aggregationType = aggregationType;
        this.nesting = nesting;
        this.consoleId = consoleId;
        this.precision = precision;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("aggregationType", aggregationType)
                .append("uniqueCountOn", uniqueCountOn)
                .append("aggregationField", aggregationField)
                .append("consoleId", consoleId)
                .append("nesting", nesting)
                .toString();
    }
}
