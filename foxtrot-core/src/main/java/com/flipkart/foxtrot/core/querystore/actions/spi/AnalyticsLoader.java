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
package com.flipkart.foxtrot.core.querystore.actions.spi;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 12:20 AM
 */
public class AnalyticsLoader {

    private final Map<String, ActionMetadata> actions = Maps.newHashMap();
    private final DataStore dataStore;
    private final ElasticsearchConnection elasticsearchConnection;

    public AnalyticsLoader(DataStore dataStore,
                           ElasticsearchConnection elasticsearchConnection) {
        this.dataStore = dataStore;
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @SuppressWarnings("unchecked")
    public <R extends ActionRequest> Action<R>
    getAction(R request) throws Exception {
        final String className = request.getClass().getCanonicalName();
        if (actions.containsKey(className)) {
            ActionMetadata metadata = actions.get(className);
            if (metadata.getRequest().isInstance(request)) {
                R r = (R) metadata.getRequest().cast(request);
                Constructor<? extends Action> constructor
                        = metadata.getAction().getConstructor(metadata.getRequest(),
                        DataStore.class,
                        ElasticsearchConnection.class,
                        String.class);
                return constructor.newInstance(r, dataStore, elasticsearchConnection, metadata.getCacheToken());
            }
        }
        return null;
    }

    public void register(ActionMetadata actionMetadata) {
        actions.put(actionMetadata.getRequest().getCanonicalName(), actionMetadata);
        if (actionMetadata.isCacheable()) {
            CacheUtils.create(actionMetadata.getCacheToken());
        }
    }

}
