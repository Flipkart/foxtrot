package com.flipkart.foxtrot.common.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.VacantESNodeGroup;

public interface ESNodeGroupVisitor<T> {

    T visit(AllocatedESNodeGroup allocatedESNodeGroup);

    T visit(VacantESNodeGroup vacantESNodeGroup);

}
