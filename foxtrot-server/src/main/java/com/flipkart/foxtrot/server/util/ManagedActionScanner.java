package com.flipkart.foxtrot.server.util;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Map<Class<?>, Class<?>> maps = Maps.newHashMap();
        for(Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if(null == analyticsProvider.request()
                    || null == analyticsProvider.opcode()
                    || analyticsProvider.opcode().isEmpty()) {
                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            }
            analyticsLoader.register(new ActionMetadata(
                    analyticsProvider.request(), action,
                    analyticsProvider.cacheable(), analyticsProvider.opcode()));
            maps.put(action, Action.class);
            logger.info("Registered action: " + action.getCanonicalName());
        }
        environment.getObjectMapperFactory().setMixinAnnotations(maps);
    }

    @Override
    public void stop() throws Exception {

    }

}
