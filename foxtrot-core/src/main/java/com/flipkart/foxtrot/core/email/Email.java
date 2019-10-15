package com.flipkart.foxtrot.core.email;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 *
 */
@Data
@Builder
public class Email {
    private final String subject;
    private final String content;
    private final List<String> recipients;
}
