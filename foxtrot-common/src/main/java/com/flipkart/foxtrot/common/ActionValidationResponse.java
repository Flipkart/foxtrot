package com.flipkart.foxtrot.common;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

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
