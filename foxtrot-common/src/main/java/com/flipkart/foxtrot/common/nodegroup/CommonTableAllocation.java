package com.flipkart.foxtrot.common.nodegroup;

import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.COMMON;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class CommonTableAllocation extends TableAllocation {

    public CommonTableAllocation() {
        super(COMMON);
    }

    @Builder
    public CommonTableAllocation(int totalShardsPerNode) {
        super(COMMON, totalShardsPerNode);
    }

    @Override
    public <T> T accept(TableAllocationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}