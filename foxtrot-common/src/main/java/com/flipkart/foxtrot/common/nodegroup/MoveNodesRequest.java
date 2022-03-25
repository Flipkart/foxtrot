package com.flipkart.foxtrot.common.nodegroup;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MoveNodesRequest {

    private String sourceGroup;

    private String destinationGroup;

    private List<String> nodePatterns;
}
