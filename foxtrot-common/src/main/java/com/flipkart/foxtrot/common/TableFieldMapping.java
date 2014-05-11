package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Set;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMapping {
    private String table;
    private Set<FieldTypeMapping> mappings;

    public TableFieldMapping() {
    }

    public TableFieldMapping(String table, Set<FieldTypeMapping> mappings) {
        this.table = table;
        this.mappings = mappings;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Set<FieldTypeMapping> getMappings() {
        return mappings;
    }

    public void setMappings(Set<FieldTypeMapping> mappings) {
        this.mappings = mappings;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("mappings", mappings)
                .toString();
    }
}
