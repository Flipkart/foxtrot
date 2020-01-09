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
package com.flipkart.foxtrot.core.querystore.actions.spi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.AnalyticsActionLoaderException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.dropwizard.lifecycle.Managed;
import lombok.Getter;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 12:20 AM
 */

@Getter
@Singleton
@Order(20)
public class AnalyticsLoader implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsLoader.class);

    private final Map<String, ActionMetadata> actions = Maps.newHashMap();
    private final TableMetadataManager tableMetadataManager;
    private final DataStore dataStore;
    private final QueryStore queryStore;
    private final ElasticsearchConnection elasticsearchConnection;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    @Inject
    public AnalyticsLoader(
            TableMetadataManager tableMetadataManager, DataStore dataStore, QueryStore queryStore,
            ElasticsearchConnection elasticsearchConnection, CacheManager cacheManager, ObjectMapper objectMapper,
            ElasticsearchTuningConfig elasticsearchTuningConfig) {
        this.tableMetadataManager = tableMetadataManager;
        this.dataStore = dataStore;
        this.queryStore = queryStore;
        this.elasticsearchConnection = elasticsearchConnection;
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.elasticsearchTuningConfig = elasticsearchTuningConfig;
    }

    @SuppressWarnings("unchecked")
    public <R extends ActionRequest> Action<R> getAction(R request) {
        final String className = request.getClass()
                .getCanonicalName();
        if (actions.containsKey(className)) {
            ActionMetadata metadata = actions.get(className);
            if (metadata.getRequest()
                    .isInstance(request)) {
                R r = (R) metadata.getRequest()
                        .cast(request);
                try {
                    Constructor<? extends Action> constructor = metadata.getAction()
                            .getConstructor(metadata.getRequest(), AnalyticsLoader.class);
                    return constructor.newInstance(r, this);
                }
                catch (Exception e) {
                    throw FoxtrotExceptions.createActionResolutionException(request, e);
                }
            }
        }
        return null;
    }

    public void register(ActionMetadata actionMetadata, String opcode) {
        actions.put(actionMetadata.getRequest().getCanonicalName(), actionMetadata);
        if (actionMetadata.isCacheable()) {
            registerCache(opcode);
        }
    }

    public void registerCache(final String opcode) {
        cacheManager.create(opcode);
    }

    @Override
    public void start() throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actionSet = reflections.getSubTypesOf(Action.class);
        if (actionSet.isEmpty()) {
            throw new AnalyticsActionLoaderException("No analytics actions found!!");
        }
        List<NamedType> types = new ArrayList<>();
        for (Class<? extends Action> action : actionSet) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            final String opcode = analyticsProvider.opcode();
            if (Strings.isNullOrEmpty(opcode)) {
                throw new AnalyticsActionLoaderException("Invalid annotation on " + action.getCanonicalName());
            }
            register(new ActionMetadata(analyticsProvider.request(), action, analyticsProvider.cacheable()),
                     analyticsProvider.opcode());
            types.add(new NamedType(analyticsProvider.request(), opcode));
            types.add(new NamedType(analyticsProvider.response(), opcode));
            logger.info("Registered action: {}", action.getCanonicalName());
        }
        objectMapper.getSubtypeResolver()
                .registerSubtypes(types.toArray(new NamedType[0]));
    }

    @Override
    public void stop() throws Exception {
        //do nothing
    }
}
