package com.flipkart.foxtrot.core.util;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.funnel.model.visitor.FunnelExtrapolationValidator;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunnelExtrapolationUtils {

    public static final String FUNNEL_ID_QUERY_FIELD = "eventData.funnelInfo.funnelId";

    public static final List<String> VALID_STATS_FOR_EXTRAPOLATION = Arrays.asList(Utils.COUNT, Utils.SUM,
            Utils.SUM_OF_SQUARES);

    private FunnelExtrapolationUtils() {

    }

    public static Optional<Long> extractFunnelId(ActionRequest actionRequest) {
        FunnelExtrapolationValidator extrapolationValidator = new FunnelExtrapolationValidator();
        Boolean extrapolationApplicable = actionRequest.accept(extrapolationValidator);
        if (Objects.nonNull(extrapolationApplicable) && extrapolationApplicable) {
            // TODO: Extract funnelId from eventType when funnelId is not given in filter
            // Extract funnel id if equals filter is applied on eventData.funnelInfo.funnelId
            try {
                Optional<Filter> funnelIdFilter = actionRequest.getFilters()
                        .stream()
                        .filter(filter -> (filter instanceof EqualsFilter) && (filter.getField()
                                .equals(FUNNEL_ID_QUERY_FIELD)))
                        .findFirst();
                if (funnelIdFilter.isPresent()) {
                    Long funnelId = Long.parseLong((String) (((EqualsFilter) funnelIdFilter.get()).getValue()));
                    log.debug("Extrapolation applicable with funnel id :{} for actionRequest: {}", funnelId,
                            actionRequest);
                    return Optional.of(funnelId);
                }
            } catch (NumberFormatException ex) {
                log.error("Error while extracting funnel id from action request : {} ", actionRequest, ex);
            }
        }
        log.debug("Extrapolation not applicable for actionRequest: {}", actionRequest);
        return Optional.empty();

    }

}
