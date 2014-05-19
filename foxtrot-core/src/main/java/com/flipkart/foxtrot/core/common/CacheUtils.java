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
package com.flipkart.foxtrot.core.common;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 5:20 PM
 */
public class CacheUtils {

    private static final Map<String, Cache> cacheMap = new HashMap<String, Cache>();

    private static CacheFactory cacheFactory = null;

    public static void setCacheFactory(CacheFactory cacheFactory) {
        CacheUtils.cacheFactory = cacheFactory;
    }

    public static void create(String name) {
        cacheMap.put(name, cacheFactory.create(name));
    }

    public static Cache getCacheFor(String name) {
        return cacheMap.get(name);
    }
}
