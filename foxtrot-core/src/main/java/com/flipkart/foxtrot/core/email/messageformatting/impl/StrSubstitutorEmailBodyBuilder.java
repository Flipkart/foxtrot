package com.flipkart.foxtrot.core.email.messageformatting.impl;

import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.text.StrSubstitutor;

/**
 *
 */
public class StrSubstitutorEmailBodyBuilder implements EmailBodyBuilder {

    private static final Map<String, String> TEMPLATES = ImmutableMap.<String, String>builder().put(
            "query_processing_error_cardinality_overflow",
            "Blocked Query: ${requestStr}\n"
                    + "Console Id: ${consoleId}\n"
                    +"Suspect field: ${field}\n"
                    + "Probability of screwing up the cluster: ${probability}")
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
