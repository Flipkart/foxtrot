package com.flipkart.foxtrot.core.email.messageformatting;

import java.util.Map;

/**
 *
 */
public interface EmailSubjectBuilder {
    String content(final String identifier, final Map<String, Object> context);
}
