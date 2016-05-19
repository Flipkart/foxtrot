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
package com.flipkart.foxtrot.core.querystore.actions;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Created by rishabh.goyal on 14/05/14.
 */
public class Constants {

    public static final Map<String, String> rawKeyVersionToSuffixMap = ImmutableMap.<String, String>builder()
            .put("2.0", "__RAW_KEY_VERSION_2__").build();

    public static final String FIELD_REPLACEMENT_REGEX = "[^a-zA-Z0-9\\-_]";
    public static final String FIELD_REPLACEMENT_VALUE = "_";
    public static final String SEPARATOR = "_--&--_";
}
