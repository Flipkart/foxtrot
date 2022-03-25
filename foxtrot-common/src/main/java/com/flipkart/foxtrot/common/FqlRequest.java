package com.flipkart.foxtrot.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FqlRequest {

    private String query;
    private boolean extrapolationFlag;

}
