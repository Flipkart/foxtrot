package com.flipkart.foxtrot.core.util;/**
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.MapType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/***
 Created by nitish.goyal on 26/07/18
 ***/
public class ElasticsearchQueryUtils {

    public static final int QUERY_SIZE = 1000;

    public static Map<String, Object> getSourceMap(Object value, Class kClass) {
        try {
            Field[] fields = kClass.getDeclaredFields();
            Map<String, Object> sourceMap = new HashMap<String, Object>();
            for (Field f : fields) {
                f.setAccessible(true);
                sourceMap.put(f.getName(), f.get(value));
            }
            return sourceMap;
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while coverting to map", e);
        }
    }

    public static Map<String, Object> getSourceMap(ObjectNode node, ObjectMapper mapper) {
        try {
            final MapType type = mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
            return mapper.readValue(node.toString(), type);
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while coverting to map", e);
        }
    }
}
