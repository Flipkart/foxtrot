package com.flipkart.foxtrot.core.queryexecutor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationValidator;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class QueryExecutorFactory {

    private final QueryExecutor simpleQueryExecutor;
    private final ExtrapolationQueryExecutor extrapolationQueryExecutor;
    private final FunnelExtrapolationValidator funnelExtrapolationValidator;

    @Inject
    public QueryExecutorFactory(final AnalyticsLoader analyticsLoader,
                                final ExecutorService executorService,
                                final List<ActionExecutionObserver> executionObservers,
                                final FunnelConfiguration funnelConfiguration) {
        this.funnelExtrapolationValidator = new FunnelExtrapolationValidator();
        this.simpleQueryExecutor = new SimpleQueryExecutor(analyticsLoader, executorService, executionObservers);

        this.extrapolationQueryExecutor = new ExtrapolationQueryExecutor(analyticsLoader, executorService,
                executionObservers, simpleQueryExecutor, funnelConfiguration);
    }

    public <T extends ActionRequest> QueryExecutor getExecutor(T request) {
        return request.accept(funnelExtrapolationValidator)
               ? extrapolationQueryExecutor
               : simpleQueryExecutor;
    }


}
