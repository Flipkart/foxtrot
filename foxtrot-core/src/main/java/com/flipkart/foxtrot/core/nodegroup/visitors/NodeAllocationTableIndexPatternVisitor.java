package com.flipkart.foxtrot.core.nodegroup.visitors;

import com.flipkart.foxtrot.common.nodegroup.CommonTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getIndexPrefix;

public class NodeAllocationTableIndexPatternVisitor implements TableAllocationVisitor<List<String>> {

    private static final String ALL_FOXTROT_INDEX_PATTERN = "foxtrot-*";

    @Override
    public List<String> visit(SpecificTableAllocation tableAllocation) {
        return tableAllocation.getTables()
                .stream()
                .map(table -> String.format("%s*", getIndexPrefix(table)))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> visit(CommonTableAllocation tableAllocation) {
        return Lists.newArrayList(ALL_FOXTROT_INDEX_PATTERN);
    }
}
