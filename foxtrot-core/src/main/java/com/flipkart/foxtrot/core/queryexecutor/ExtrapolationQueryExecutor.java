package com.flipkart.foxtrot.core.queryexecutor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationResponseVisitor;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;

@Slf4j
public class ExtrapolationQueryExecutor extends QueryExecutor {

    private final QueryExecutor simpleQueryExecutor;

    private final FunnelConfiguration funnelConfiguration;

    private final FunnelStore funnelStore;

    public ExtrapolationQueryExecutor(final AnalyticsLoader analyticsLoader,
                                      final ExecutorService executorService,
                                      final List<ActionExecutionObserver> executionObservers,
                                      final QueryExecutor queryExecutor,
                                      final FunnelConfiguration funnelConfiguration,
                                      final FunnelStore funnelStore) {
        super(analyticsLoader, executorService, executionObservers);
        this.simpleQueryExecutor = queryExecutor;
        this.funnelConfiguration = funnelConfiguration;
        this.funnelStore = funnelStore;
    }

    @Override
    protected <T extends ActionRequest> ActionResponse execute(T actionRequest,
                                                               Action action) {
        ActionResponse originalResponse = action.execute();
        FunnelExtrapolationResponseVisitor extrapolationResponseVisitor = new FunnelExtrapolationResponseVisitor(
                actionRequest, simpleQueryExecutor, funnelStore, funnelConfiguration);
        return originalResponse.accept(extrapolationResponseVisitor);
    }
}
