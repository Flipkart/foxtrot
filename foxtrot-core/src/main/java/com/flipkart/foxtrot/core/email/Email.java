package com.flipkart.foxtrot.core.email;

import java.util.List;
import lombok.Builder;
import lombok.Data;

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
