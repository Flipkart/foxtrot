package com.flipkart.foxtrot.common;

import lombok.*;

import java.util.List;

/**
 * Response for validation API
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ActionValidationResponse {
    private ActionRequest processedRequest;
    @Singular
    private List<String> validationErrors;
}
