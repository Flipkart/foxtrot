package com.flipkart.foxtrot.core.util;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/***
 Created by nitish.goyal on 26/07/18
 ***/
public class ElasticsearchQueryUtils {

    public static final int QUERY_SIZE = 10000;

    private ElasticsearchQueryUtils() {
    }

    public static Map<String, Object> toMap(ObjectMapper mapper, Object value) {
        return mapper.convertValue(value, new TypeReference<Map<String, Object>>() {
        });
    }
}
