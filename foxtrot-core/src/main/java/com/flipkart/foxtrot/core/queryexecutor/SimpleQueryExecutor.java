package com.flipkart.foxtrot.core.queryexecutor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SimpleQueryExecutor extends QueryExecutor {

    @Inject
    public SimpleQueryExecutor(final AnalyticsLoader analyticsLoader,
                               final ExecutorService executorService,
                               final List<ActionExecutionObserver> executionObservers) {
        super(analyticsLoader, executorService, executionObservers);
    }

    @Override
    protected <T extends ActionRequest> ActionResponse execute(T request,
                                                               Action action) {
        return action.execute();
    }
}
