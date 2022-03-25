package com.flipkart.foxtrot.core.nodegroup.repository;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus;
import com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType;
import com.flipkart.foxtrot.common.nodegroup.VacantESNodeGroup;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryNodeGroupRepository implements NodeGroupRepository {

    private Map<String, ESNodeGroup> nodeGroupMap;

    public InMemoryNodeGroupRepository() {
        nodeGroupMap = new HashMap<>();
    }

    @Override
    public void save(ESNodeGroup nodeGroup) {
        nodeGroupMap.put(nodeGroup.getGroupName(), nodeGroup);
    }

    @Override
    public ESNodeGroup get(String groupName) {
        return nodeGroupMap.get(groupName);
    }

    @Override
    public VacantESNodeGroup getVacantGroup() {
        return nodeGroupMap.values()
                .stream()
                .filter(nodeGroup -> nodeGroup.getStatus()
                        .equals(AllocationStatus.VACANT))
                .map(esNodeGroup -> (VacantESNodeGroup) esNodeGroup)
                .findFirst()
                .orElse(null);
    }

    @Override
    public AllocatedESNodeGroup getCommonGroup() {
        return nodeGroupMap.values()
                .stream()
                .filter(nodeGroup -> nodeGroup.getStatus()
                        .equals(AllocationStatus.ALLOCATED))
                .filter(nodeGroup -> ((AllocatedESNodeGroup) nodeGroup).getTableAllocation()
                        .getType()
                        .equals(TableAllocationType.COMMON))
                .map(AllocatedESNodeGroup.class::cast)
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<ESNodeGroup> getAll() {
        return Lists.newArrayList(nodeGroupMap.values());
    }

    @Override
    public void delete(String groupName) {
        nodeGroupMap.remove(groupName);
    }
}
