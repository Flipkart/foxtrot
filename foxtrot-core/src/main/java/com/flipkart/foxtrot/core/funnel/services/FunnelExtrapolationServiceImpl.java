package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationValidator;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelResponseVisitorAdapter;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class FunnelExtrapolationServiceImpl implements FunnelExtrapolationService {

    private final FunnelExtrapolationValidator extrapolationValidator;
    private final QueryExecutor queryExecutor;
    private final BaseFunnelEventConfig baseFunnelEventConfig;

    @Inject
    public FunnelExtrapolationServiceImpl(final BaseFunnelEventConfig funnelEventConfig,
            @Named("SimpleQueryExecutor") final QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        this.baseFunnelEventConfig = funnelEventConfig;
        this.extrapolationValidator = new FunnelExtrapolationValidator();
    }

    public ActionResponse extrapolateResponse(ActionRequest actionRequest, ActionResponse originalResponse) {
        if (actionRequest.accept(extrapolationValidator)) {
            FunnelResponseVisitorAdapter responseVisitorAdapter = new FunnelResponseVisitorAdapter(actionRequest,
                    queryExecutor, baseFunnelEventConfig);
            return originalResponse.accept(responseVisitorAdapter);
        }

        return originalResponse;
    }


}
