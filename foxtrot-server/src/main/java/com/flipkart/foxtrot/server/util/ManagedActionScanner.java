package com.flipkart.foxtrot.server.util;

import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.yammer.dropwizard.lifecycle.Managed;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:23 AM
 */
public class ManagedActionScanner implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ManagedActionScanner.class.getSimpleName());

    private AnalyticsLoader analyticsLoader;

    public ManagedActionScanner(AnalyticsLoader analyticsLoader) {
        this.analyticsLoader = analyticsLoader;
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
    }

    @Override
    public void stop() throws Exception {

    }

}
