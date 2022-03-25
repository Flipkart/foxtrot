package com.flipkart.foxtrot.common.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;

public interface TableAllocationVisitor<T> {

    T visit(SpecificTableAllocation tableAllocation);

    T visit(CommonTableAllocation tableAllocation);
}
