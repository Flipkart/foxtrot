package com.flipkart.foxtrot.server.guice;

import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.util.List;

public class ActionExecutionObserverListProvider implements Provider<List<ActionExecutionObserver>> {

    @Inject
    private Provider<MetricRecorder> metricRecorderProvider;

    @Inject
    private Provider<ResponseCacheUpdater> responseCacheUpdaterProvider;

    @Inject
    private Provider<SlowQueryReporter> slowQueryReporterProvider;

    @Inject
    private Provider<EventPublisherActionExecutionObserver> eventPublisherActionExecutionObserverProvider;

    public List<ActionExecutionObserver> get() {
        return ImmutableList.<ActionExecutionObserver>builder()
                .add(metricRecorderProvider.get())
                .add(responseCacheUpdaterProvider.get())
                .add(slowQueryReporterProvider.get())
                .add(eventPublisherActionExecutionObserverProvider.get())
                .build();
    }
}
