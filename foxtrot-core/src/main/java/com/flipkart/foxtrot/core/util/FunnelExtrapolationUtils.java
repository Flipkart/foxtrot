package com.flipkart.foxtrot.core.util;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.exception.FunnelException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FunnelExtrapolationUtils {

    public static final String FUNNEL_ID_QUERY_FIELD = "eventData.funnelInfo.funnelId";

    protected static final List<String> VALID_STATS_FOR_EXTRAPOLATION = Arrays.asList(Utils.COUNT, Utils.SUM,
            Utils.SUM_OF_SQUARES);

    private FunnelExtrapolationUtils() {
        throw new IllegalStateException("Utility Class");
    }

    public static Long ensureFunnelId(ActionRequest actionRequest) {
        Optional<Long> funnelIdOptional = extractFunnelId(actionRequest);
        if (!funnelIdOptional.isPresent()) {
            log.error("Funnel id not present, function called without validation for action request :{}",
                    actionRequest);
            throw new FunnelException("Funnel id not present in action request");
        }
        return funnelIdOptional.get();
    }

    public static Optional<Long> extractFunnelId(ActionRequest actionRequest) {
        // Extract funnel id if equals filter is applied on eventData.funnelInfo.funnelId
        try {
            Optional<Filter> funnelIdFilter = actionRequest.getFilters()
                    .stream()
                    .filter(filter -> (filter instanceof EqualsFilter) && (filter.getField()
                            .equals(FUNNEL_ID_QUERY_FIELD)))
                    .findFirst();
            if (funnelIdFilter.isPresent()) {
                Long funnelId = Long.parseLong((((EqualsFilter) funnelIdFilter.get()).getValue()).toString());
                return Optional.of(funnelId);
            }
        } catch (NumberFormatException ex) {
            log.error("Error while extracting funnel id from action request : {} ", actionRequest, ex);
        }
        return Optional.empty();
    }

    public static void printFunnelNotApplicableLog(ActionRequest actionRequest) {
        log.debug("Extrapolation not applicable for actionRequest: {}", actionRequest);
    }

    public static void printFunnelApplicableLog(ActionRequest actionRequest,
                                                Long funnelId) {
        log.debug("Extrapolation applicable with funnel id :{} for actionRequest: {}", funnelId, actionRequest);
    }

    public static List<String> getValidStatsForExtrapolation() {
        return Collections.unmodifiableList(VALID_STATS_FOR_EXTRAPOLATION);
    }
}
