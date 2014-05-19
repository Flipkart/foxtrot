/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
