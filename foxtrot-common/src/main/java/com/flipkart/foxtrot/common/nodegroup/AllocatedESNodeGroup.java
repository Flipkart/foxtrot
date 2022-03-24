package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.nodegroup.visitors.ESNodeGroupVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collections;
import java.util.SortedSet;

import static com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus.ALLOCATED;
import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.COMMON;
import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.SPECIFIC;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AllocatedESNodeGroup extends ESNodeGroup {

    private TableAllocation tableAllocation;

    public AllocatedESNodeGroup() {
        super(ALLOCATED);
    }

    @Builder
    public AllocatedESNodeGroup(String groupName,
                                SortedSet<String> nodePatterns,
                                TableAllocation tableAllocation) {
        super(groupName, ALLOCATED, nodePatterns);
        this.tableAllocation = tableAllocation;
    }

    public <T> T accept(ESNodeGroupVisitor<T> visitor) {
        return visitor.visit(this);
    }


    @JsonIgnore
    public boolean isAnyTableAllocationOverlappingWith(AllocatedESNodeGroup group) {
        if (COMMON.equals(this.tableAllocation.getType()) && COMMON.equals(group.getTableAllocation()
                .getType())) {
            return true;
        }
        if (SPECIFIC.equals(this.tableAllocation.getType()) && SPECIFIC.equals(group.getTableAllocation()
                .getType())) {
            SpecificTableAllocation tableAllocation1 = (SpecificTableAllocation) this.getTableAllocation();
            SpecificTableAllocation tableAllocation2 = (SpecificTableAllocation) group.getTableAllocation();
            return !Collections.disjoint(tableAllocation1.getTables(), tableAllocation2.getTables());

        }
        return false;
    }

}
