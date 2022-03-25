package com.flipkart.foxtrot.common.nodegroup.visitors;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MoveTablesRequest {

    private String sourceGroup;

    private String destinationGroup;

    private List<String> tables;
}
