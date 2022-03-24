package com.flipkart.foxtrot.core.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;

public class NodeAllocationTemplateOrderVisitor implements TableAllocationVisitor<Integer> {

    @Override
    public Integer visit(SpecificTableAllocation tableAllocation) {
        return 3;
    }

    @Override
    public Integer visit(CommonTableAllocation tableAllocation) {
        return 2;
    }
}
