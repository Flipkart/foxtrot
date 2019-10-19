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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import lombok.Data;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 9:17 PM
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "opcode")
@JsonSubTypes({@JsonSubTypes.Type(value = CountResponse.class, name = Opcodes.COUNT), @JsonSubTypes.Type(value = DistinctResponse.class,
                                                                                                         name = Opcodes.DISTINCT),
               @JsonSubTypes.Type(value = GroupResponse.class, name = Opcodes.GROUP), @JsonSubTypes.Type(value = HistogramResponse.class,
                                                                                                         name = Opcodes.HISTOGRAM),
               @JsonSubTypes.Type(value = QueryResponse.class, name = Opcodes.QUERY), @JsonSubTypes.Type(value = MultiQueryResponse
        .class, name = Opcodes.MULTI_QUERY), @JsonSubTypes.Type(value = StatsResponse.class, name = Opcodes.STATS), @JsonSubTypes.Type
                       (value = TrendResponse.class, name = Opcodes.TREND), @JsonSubTypes.Type(value = StatsTrendResponse.class, name =
        Opcodes.STATS_TREND), @JsonSubTypes.Type(value = MultiTimeQueryResponse.class, name = Opcodes.MULTI_TIME_QUERY)})
@Data
public abstract class ActionResponse {
    private final String opcode;
    private boolean fromCache;

    protected ActionResponse(String opcode) {
        this.opcode = opcode;
    }

    public abstract void accept(ResponseVisitor visitor);

}
