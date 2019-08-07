package com.flipkart.foxtrot.core.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TextNodeRemoverConfiguration {


    @Min(0)
    @Max(100)
    @Builder.Default
    private int blockPercentage = 0;

    @Builder.Default
    private int maxAllowedSize = 1024;

    @Min(0)
    @Max(100)
    @Builder.Default
    private int logSamplingPercentage = 0;

}
