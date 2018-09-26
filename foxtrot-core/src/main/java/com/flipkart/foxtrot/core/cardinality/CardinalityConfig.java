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

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.DefaultValue;

/***
 Created by nitish.goyal on 06/08/18
 ***/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CardinalityConfig extends BaseJobConfig {

    private static final String JOB_NAME = "CardinalityConfigJob";

    private String enabled;
    private String batchSize;

    @DefaultValue("50000")
    private long maxCardinality;

    public CardinalityConfig(String enabled, String batchSize) {
        this.enabled = enabled;
        this.batchSize = batchSize;
    }

    public boolean isEnabled() {
        if(StringUtils.isEmpty(enabled)) {
            return false;
        }
        return Boolean.valueOf(enabled);
    }

    public int getSubListSize() {
        if(StringUtils.isEmpty(batchSize)) {
            return ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE;
        }
        return Integer.parseInt(batchSize);
    }

    @Override
    public String getJobName() {
        return JOB_NAME;
    }
}
