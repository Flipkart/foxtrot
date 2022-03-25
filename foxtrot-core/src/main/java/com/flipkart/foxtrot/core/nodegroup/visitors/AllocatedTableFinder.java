package com.flipkart.foxtrot.core.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;

import java.util.Set;
import java.util.stream.Collectors;

public class AllocatedTableFinder implements TableAllocationVisitor<Set<String>> {

    private Set<String> allTables;

    private Set<String> specificAllocatedTables;

    public AllocatedTableFinder(final Set<String> allTables,
                                final Set<String> specificAllocatedTables) {
        this.allTables = allTables;
        this.specificAllocatedTables = specificAllocatedTables;
    }

    @Override
    public Set<String> visit(SpecificTableAllocation tableAllocation) {
        return allTables.stream()
                .filter(table -> tableAllocation.getTables()
                        .contains(table))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> visit(CommonTableAllocation tableAllocation) {
        return allTables.stream()
                .filter(table -> !specificAllocatedTables.contains(table))
                .collect(Collectors.toSet());
    }
}
