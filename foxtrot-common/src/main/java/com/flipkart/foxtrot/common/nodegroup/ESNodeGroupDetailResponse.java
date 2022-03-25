package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Builder;
import lombok.Data;

@Data
public class ESNodeGroupDetailResponse {

    @JsonUnwrapped
    private ESNodeGroup nodeGroup;

    private ESNodeGroupDetails details;

    @Builder
    public ESNodeGroupDetailResponse(ESNodeGroup nodeGroup,
                                     ESNodeGroupDetails details) {
        this.nodeGroup = nodeGroup;
        this.details = details;
    }

}
