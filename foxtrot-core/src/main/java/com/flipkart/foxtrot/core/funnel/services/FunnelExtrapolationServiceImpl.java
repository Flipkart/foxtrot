package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationValidator;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelResponseVisitorAdapter;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class FunnelExtrapolationServiceImpl implements FunnelExtrapolationService {

    private final FunnelExtrapolationValidator extrapolationValidator;
    private final QueryExecutor queryExecutor;
    private final FunnelConfiguration funnelConfiguration;

    @Inject
    public FunnelExtrapolationServiceImpl(final FunnelConfiguration funnelConfiguration,
            @Named("SimpleQueryExecutor") final QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        this.funnelConfiguration = funnelConfiguration;
        this.extrapolationValidator = new FunnelExtrapolationValidator();
    }

    public ActionResponse extrapolateResponse(ActionRequest actionRequest, ActionResponse originalResponse) {
        Boolean extrapolationApplicable = actionRequest.accept(extrapolationValidator);
        if (Objects.nonNull(extrapolationApplicable) && extrapolationApplicable) {
            FunnelResponseVisitorAdapter responseVisitorAdapter = new FunnelResponseVisitorAdapter(actionRequest,
                    queryExecutor, funnelConfiguration.getBaseFunnelEventConfig());
            return originalResponse.accept(responseVisitorAdapter);
        }

        return originalResponse;
    }


}
