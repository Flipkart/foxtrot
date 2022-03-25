package com.flipkart.foxtrot.core.email.messageformatting.impl;

import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.Map;

import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID;
import static com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter.SLOW_QUERY_EMAIL_TEMPLATE_ID;

/**
 *
 */
public class StrSubstitutorEmailBodyBuilder implements EmailBodyBuilder {

    private static final Map<String, String> TEMPLATES = ImmutableMap.<String, String>builder().put(
            "query_processing_error_cardinality_overflow",
            "Blocked Query: ${requestStr}\n" + "Console Id: ${consoleId}\n" + "Suspect fields: ${fields}\n"
                    + "Cache Key: ${cacheKey}\n" + "Probability of screwing up the cluster: ${probability}\n" + ""
                    + " Probability Calculation: ${probabilityCalculation}")
            .put(SLOW_QUERY_EMAIL_TEMPLATE_ID,
                    "Execution Time: ${executionTime}\n" + " Slow Query: ${requestStr}\n" + "Cache Key: ${cacheKey}"
                            + " Console Id: ${consoleId}\n")
            .put(CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID,
                    "Cardinality Calculation Failure\n Error message: ${message}\n" + " Cause: ${cause}\n"
                            + " Cause Message: ${causeMessage}")
            .put(CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID,
                    "Cardinality Calculation Time Limit Exceeded\n Elapsed time: ${elapsedTime} seconds\n"
                            + " Max Time To Run Job: ${maxTimeToRunJob} seconds")
            .build();

    @Override
    public String content(String identifier,
                          Map<String, Object> context) {
        if (!TEMPLATES.containsKey(identifier)) {
            return "";
        }
        return StrSubstitutor.replace(TEMPLATES.get(identifier), context);
    }
}
