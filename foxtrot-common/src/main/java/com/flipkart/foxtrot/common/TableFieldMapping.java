package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Set;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMapping {
    private String table;
    private Set<FieldTypeMapping> fieldMappings;

    public TableFieldMapping() {
    }

    public TableFieldMapping(String table, Set<FieldTypeMapping> fieldMappings) {
        this.table = table;
        this.fieldMappings = fieldMappings;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Set<FieldTypeMapping> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(Set<FieldTypeMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("fieldMappings", fieldMappings)
                .toString();
    }
}
