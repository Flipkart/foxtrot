package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(name = "SPECIFIC", value = SpecificTableAllocation.class),
        @JsonSubTypes.Type(name = "COMMON", value = CommonTableAllocation.class)})
public abstract class TableAllocation {

    private TableAllocationType type;

    private int totalShardsPerNode;

    public TableAllocation(TableAllocationType type) {
        this.type = type;
    }

    public TableAllocation(TableAllocationType type,
                           int totalShardsPerNode) {
        this.type = type;
        this.totalShardsPerNode = totalShardsPerNode;
    }

    public abstract <T> T accept(TableAllocationVisitor<T> visitor);

    public enum TableAllocationType {
        SPECIFIC,
        COMMON
    }

}
