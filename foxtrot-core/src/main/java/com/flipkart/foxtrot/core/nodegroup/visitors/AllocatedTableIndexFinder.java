package com.flipkart.foxtrot.core.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;

import java.util.Set;
import java.util.stream.Collectors;

public class AllocatedTableIndexFinder implements TableAllocationVisitor<Set<String>> {

    private Set<String> allIndices;

    private Set<String> specificAllocatedTables;

    public AllocatedTableIndexFinder(final Set<String> allIndices,
                                     final Set<String> specificAllocatedTables) {
        this.allIndices = allIndices;
        this.specificAllocatedTables = specificAllocatedTables;
    }

    @Override
    public Set<String> visit(SpecificTableAllocation tableAllocation) {
        return allIndices.stream()
                .filter(tableIndex -> tableAllocation.getTables()
                        .contains(ElasticsearchUtils.getTableNameFromIndex(tableIndex)))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> visit(CommonTableAllocation tableAllocation) {
        return allIndices.stream()
                .filter(tableIndex -> !specificAllocatedTables.contains(
                        ElasticsearchUtils.getTableNameFromIndex(tableIndex)))
                .collect(Collectors.toSet());
    }
}
