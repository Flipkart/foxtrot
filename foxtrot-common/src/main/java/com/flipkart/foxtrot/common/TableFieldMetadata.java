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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Set;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMetadata {
    private String table;
    private Set<FieldData> fieldData;

    public TableFieldMetadata() {
    }

    public TableFieldMetadata(String table, Set<FieldData> fieldData) {
        this.table = table;
        this.fieldData = fieldData;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Set<FieldData> getFieldData() {
        return fieldData;
    }

    public void setFieldData(Set<FieldData> fieldData) {
        this.fieldData = fieldData;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("fieldMetadata", fieldData)
                .toString();
    }
}
