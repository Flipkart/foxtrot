package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.common.nodegroup.visitors.TableAllocationVisitor;
import io.dropwizard.validation.ValidationMethod;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.SortedSet;

import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.SPECIFIC;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SpecificTableAllocation extends TableAllocation {

    private SortedSet<String> tables;

    public SpecificTableAllocation() {
        super(SPECIFIC);
    }

    @Builder
    public SpecificTableAllocation(SortedSet<String> tables,
                                   int totalShardsPerNode) {
        super(SPECIFIC, totalShardsPerNode);
        this.tables = tables;
    }

    @JsonIgnore
    @ValidationMethod(message = "Table name can't be a wildcard expression")
    public boolean isValid() {
        return tables.stream()
                .noneMatch(table -> table.contains("*"));
    }

    @Override
    public <T> T accept(TableAllocationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
