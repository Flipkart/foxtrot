package com.flipkart.foxtrot.server.util;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.spi.AnalyticsRequest;
import com.flipkart.foxtrot.common.spi.AnalyticsResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:23 AM
 */
public class ManagedActionScanner implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ManagedActionScanner.class.getSimpleName());

    private AnalyticsLoader analyticsLoader;
    private Environment environment;

    public ManagedActionScanner(AnalyticsLoader analyticsLoader, Environment environment) {
        this.analyticsLoader = analyticsLoader;
        this.environment = environment;
    }

    @Override
    public void start() throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actions = reflections.getSubTypesOf(Action.class);
        if(actions.isEmpty()) {
            throw new Exception("No analytics actions found!!");
        }
        for(Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if(null == analyticsProvider.request()
                    || null == analyticsProvider.cacheToken()
                    || analyticsProvider.cacheToken().isEmpty()) {
                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            }
            if(analyticsProvider.cacheToken().equalsIgnoreCase("default")) {
                logger.warn("Action " + action.getCanonicalName() + " does not specify cache token. " +
                            "Using default cache.");
            }
            analyticsLoader.register(new ActionMetadata(
                                                    analyticsProvider.request(), action,
                                                    analyticsProvider.cacheable(), analyticsProvider.cacheToken()));
            logger.info("Registered action: " + action.getCanonicalName());
        }
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        scanRequests(reflections, subtypeResolver);
        scanResponses(reflections, subtypeResolver);
        environment.getObjectMapperFactory().setSubtypeResolver(subtypeResolver);
    }

    @Override
    public void stop() throws Exception {

    }

    private void scanRequests(Reflections reflections, SubtypeResolver subtypeResolver) {
        Set<Class<? extends ActionRequest>> requestClasses = reflections.getSubTypesOf(ActionRequest.class);
        List<NamedType> types = new Vector<NamedType>();
        for(Class<?> requestClass : requestClasses) {
            String name = requestClass.getSimpleName();
            AnalyticsRequest actionRequest = requestClass.getAnnotation(AnalyticsRequest.class);
            if(null != actionRequest) {
                name = actionRequest.value();
            }
            types.add(new NamedType(requestClass, name));
            logger.info("Added response " + requestClass.getSimpleName() + ": " + name);
        }
        subtypeResolver.registerSubtypes(types.toArray(new NamedType[types.size()]));
    }

    private void scanResponses(Reflections reflections, SubtypeResolver subtypeResolver) {
        Set<Class<? extends ActionResponse>> requestClasses = reflections.getSubTypesOf(ActionResponse.class);
        List<NamedType> types = new Vector<NamedType>();
        for(Class<?> requestClass : requestClasses) {
            String name = requestClass.getSimpleName();
            AnalyticsResponse actionRequest = requestClass.getAnnotation(AnalyticsResponse.class);
            if(null != actionRequest) {
                name = actionRequest.value();
            }
            types.add(new NamedType(requestClass, name));
            logger.info("Added response " + requestClass.getSimpleName() + ": " + name);
        }
        subtypeResolver.registerSubtypes(types.toArray(new NamedType[types.size()]));
    }

}
