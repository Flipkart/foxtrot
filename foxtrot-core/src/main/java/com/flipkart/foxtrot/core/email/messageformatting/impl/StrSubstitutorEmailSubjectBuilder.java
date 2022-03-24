package com.flipkart.foxtrot.core.email.messageformatting.impl;

import com.flipkart.foxtrot.core.email.messageformatting.EmailSubjectBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.Map;

import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID;
import static com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter.SLOW_QUERY_EMAIL_TEMPLATE_ID;

/**
 *
 */
public class StrSubstitutorEmailSubjectBuilder implements EmailSubjectBuilder {

    private static final Map<String, String> TEMPLATES = ImmutableMap.of("query_processing_error_cardinality_overflow",
            "Blocked query as it might have screwed up the cluster", SLOW_QUERY_EMAIL_TEMPLATE_ID,
            "Slow foxtrot query detected", CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID,
            "Cardinality Calculation Failure Detected", CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID,
            "Cardinality Calculation Time Limit Exceeded");

    @Override
    public String content(String identifier,
                          Map<String, Object> context) {
        if (!TEMPLATES.containsKey(identifier)) {
            return "";
        }
        return StrSubstitutor.replace(TEMPLATES.get(identifier), context);
    }
}
