package com.flipkart.foxtrot.core.nodegroup;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetailResponse;
import com.flipkart.foxtrot.common.nodegroup.visitors.MoveTablesRequest;

import java.util.List;

public interface NodeGroupManager {

    ESNodeGroup createNodeGroup(ESNodeGroup nodeGroup);

    ESNodeGroup getNodeGroup(String groupName);

    AllocatedESNodeGroup getNodeGroupByTable(String table);

    void deleteNodeGroup(String groupName);

    List<ESNodeGroup> getNodeGroups();

    ESNodeGroup updateNodeGroup(String groupName,
                                ESNodeGroup nodeGroup);

    void moveTablesBetweenGroups(MoveTablesRequest moveTablesRequest);

    void syncAllocation(String groupName);

    ESNodeGroupDetailResponse getNodeGroupDetails(String groupName);

    List<ESNodeGroupDetailResponse> getNodeGroupDetails();

}
