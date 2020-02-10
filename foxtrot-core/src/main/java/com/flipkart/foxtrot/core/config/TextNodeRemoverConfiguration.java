package com.flipkart.foxtrot.core.config;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
