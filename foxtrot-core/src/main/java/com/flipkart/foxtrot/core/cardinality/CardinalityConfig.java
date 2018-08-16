package com.flipkart.foxtrot.core.cardinality;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;

/***
 Created by nitish.goyal on 06/08/18
 ***/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CardinalityConfig {

    private String cardinality;
    private String listSize;

    private boolean cardinalityEnabled;

    private int subListSize;

    @DefaultValue("50000")
    private long maxCardinality;

    @Min(3600)
    private int interval;

    /*
    Initial day in hours. Used to run the config at ith hour of the day
     */
    @Min(1)
    @JsonProperty("initialdelay")
    private int initialDelay;

    @NotNull
    private boolean active;

    public CardinalityConfig(String cardinality, String listSize) {
        this.cardinality = cardinality;
        this.listSize = listSize;
    }

    public boolean isCardinalityEnabled() {
        if (StringUtils.isEmpty(cardinality)) {
            return false;
        }
        return Boolean.valueOf(cardinality);
    }

    public int getSubListSize() {
        if (StringUtils.isEmpty(listSize)) {
            return ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE;
        }
        return Integer.valueOf(listSize);
    }
}
