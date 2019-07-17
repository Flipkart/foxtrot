package com.flipkart.foxtrot.core.alerts;

import lombok.Builder;
import lombok.Data;

/**
 *
 */
@Data
@Builder
public class AlertEmail {
    private final String subject;
    private final String content;
    private final String recipients;
}
