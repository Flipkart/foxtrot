package com.flipkart.foxtrot.core.queryexecutor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationResponseVisitor;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExtrapolationQueryExecutor extends QueryExecutor {

    private final long funnelId;

    private final QueryExecutor simpleQueryExecutor;

    private final FunnelConfiguration funnelConfiguration;

    public ExtrapolationQueryExecutor(final AnalyticsLoader analyticsLoader,
                                      final ExecutorService executorService,
                                      final List<ActionExecutionObserver> executionObservers,
                                      final long funnelId,
                                      final QueryExecutor queryExecutor,
                                      final FunnelConfiguration funnelConfiguration) {
        super(analyticsLoader, executorService, executionObservers);
        this.funnelId = funnelId;
        this.simpleQueryExecutor = queryExecutor;
        this.funnelConfiguration = funnelConfiguration;
    }

    @Override
    protected <T extends ActionRequest> ActionResponse execute(T actionRequest,
                                                               Action action) {
        ActionResponse originalResponse = action.execute();
        FunnelExtrapolationResponseVisitor extrapolationResponseVisitor = new FunnelExtrapolationResponseVisitor(
                funnelId, actionRequest, simpleQueryExecutor, funnelConfiguration.getBaseFunnelEventConfig());
        return originalResponse.accept(extrapolationResponseVisitor);
    }
}
