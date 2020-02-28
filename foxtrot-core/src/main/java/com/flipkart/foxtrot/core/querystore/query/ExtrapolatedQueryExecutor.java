package com.flipkart.foxtrot.core.querystore.query;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.funnel.services.FunnelExtrapolationService;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ExtrapolatedQueryExecutor extends QueryExecutor {

    private final FunnelExtrapolationService extrapolationService;

    @Inject
    public ExtrapolatedQueryExecutor(final AnalyticsLoader analyticsLoader,
            final ExecutorService executorService,
            final List<ActionExecutionObserver> executionObservers,
            final FunnelExtrapolationService extrapolationService) {
        super(analyticsLoader, executorService, executionObservers);
        this.extrapolationService = extrapolationService;
    }

    @Override
    protected <T extends ActionRequest> ActionResponse execute(T request, Action action) {
        ActionResponse actionResponse = action.execute();
        return extrapolationService.extrapolateResponse(request, actionResponse);
    }
}
