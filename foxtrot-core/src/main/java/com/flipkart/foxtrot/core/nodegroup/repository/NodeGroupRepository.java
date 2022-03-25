package com.flipkart.foxtrot.core.nodegroup.repository;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.VacantESNodeGroup;

import java.util.List;

public interface NodeGroupRepository {

    void save(ESNodeGroup nodeGroup);

    ESNodeGroup get(String groupName);

    VacantESNodeGroup getVacantGroup();

    AllocatedESNodeGroup getCommonGroup();

    List<ESNodeGroup> getAll();

    void delete(String groupName);
}
