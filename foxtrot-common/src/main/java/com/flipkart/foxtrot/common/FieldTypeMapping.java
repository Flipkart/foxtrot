package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class FieldTypeMapping {
    private String field;
    private FieldType type;

    public FieldTypeMapping() {
    }

    public FieldTypeMapping(String field, FieldType type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public FieldType getType() {
        return type;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("field", field)
                .append("type", type)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        FieldTypeMapping rhs = (FieldTypeMapping) obj;
        return new EqualsBuilder()
                .append(this.field, rhs.field)
                .append(this.type, rhs.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(field)
                .append(type)
                .toHashCode();
    }
}
