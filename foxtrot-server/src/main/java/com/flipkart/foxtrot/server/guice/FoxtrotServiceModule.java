package com.flipkart.foxtrot.server.guice;

import com.codahale.metrics.health.HealthCheck;
import com.flipkart.foxtrot.core.alerts.AlertingSystemEventConsumer;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class FoxtrotServiceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(FqlStoreService.class).to(FqlStoreServiceImpl.class);
        bind(InternalEventBusConsumer.class).to(AlertingSystemEventConsumer.class);
        bind(new TypeLiteral<List<ActionExecutionObserver>>() {
        }).toProvider(ActionExecutionObserverListProvider.class);
        bind(new TypeLiteral<List<HealthCheck>>() {
        }).toProvider(HealthCheckListProvider.class);
    }

    @Provides
    @Singleton
    RichEmailBuilder provideRichEmailBuilder() {
        return new RichEmailBuilder(new StrSubstitutorEmailSubjectBuilder(),
                new StrSubstitutorEmailBodyBuilder());
    }

    @Provides
    @Singleton
    InternalEventBus provideGuavaInternalEventBus(AlertingSystemEventConsumer alertingSystemEventConsumer) {
        InternalEventBus eventBus = new GuavaInternalEventBus();
        eventBus.subscribe(alertingSystemEventConsumer);
        return eventBus;
    }

    @Provides
    @Singleton
    ExecutorService provideExecutorService(Environment environment) {
        return environment.lifecycle()
                .executorService("query-executor-%s")
                .minThreads(20)
                .maxThreads(30)
                .keepAliveTime(Duration.seconds(30))
                .build();
    }

    @Provides
    @Singleton
    ScheduledExecutorService provideScheduledExecutorService(Environment environment) {
        return environment.lifecycle()
                .scheduledExecutorService("cardinality-executor")
                .threads(1)
                .build();
    }


    public static class HealthCheckListProvider implements Provider<List<HealthCheck>> {

        public List<HealthCheck> get() {
            return new ArrayList<>();
        }
    }
}
