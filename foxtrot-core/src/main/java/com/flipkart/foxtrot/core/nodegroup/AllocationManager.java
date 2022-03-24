package com.flipkart.foxtrot.core.nodegroup;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;

import java.util.Set;

public interface AllocationManager {

    void createNodeAllocationTemplate(AllocatedESNodeGroup esNodeGroup);

    void deleteNodeAllocationTemplate(String groupName);

    void syncAllocationSettings(Set<String> indices,
                                AllocatedESNodeGroup esNodeGroup);

}
