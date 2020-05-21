package com.flipkart.foxtrot.core.queryexecutor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class QueryExecutorFactory {

    private static final int POOL_EXPIRY = 300;
    private static final int POOL_SIZE = 1000;
    private final QueryExecutor simpleQueryExecutor;
    // Flyweight pool to not create unnecessary new objects for every request
    // with same funnel id
    private final LoadingCache<Long, QueryExecutor> extrapolationQueryExecutors;

    @Inject
    public QueryExecutorFactory(final AnalyticsLoader analyticsLoader,
                                final ExecutorService executorService,
                                final List<ActionExecutionObserver> executionObservers,
                                final FunnelConfiguration funnelConfiguration) {
        this.simpleQueryExecutor = new SimpleQueryExecutor(analyticsLoader, executorService, executionObservers);

        this.extrapolationQueryExecutors = Caffeine.newBuilder()
                .expireAfterWrite(POOL_EXPIRY, TimeUnit.SECONDS)
                .maximumSize(POOL_SIZE)
                .build(funnelId -> new ExtrapolationQueryExecutor(analyticsLoader, executorService, executionObservers,
                        funnelId, simpleQueryExecutor, funnelConfiguration));
    }

    public <T extends ActionRequest> QueryExecutor getExecutor(T request) {
        Optional<Long> funnelIdOptional = FunnelExtrapolationUtils.extractFunnelId(request);

        if (!funnelIdOptional.isPresent()) {
            return simpleQueryExecutor;
        } else {
            return extrapolationQueryExecutors.get(funnelIdOptional.get());
        }
    }


}
