package com.flipkart.foxtrot.common.nodegroup;

import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse.NodeFSDetails;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class VacantGroupReadRepairResult {

    private List<NodeFSDetails> dataNodes;
    private VacantESNodeGroup vacantNodeGroup;
}
