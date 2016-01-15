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
package com.flipkart.foxtrot.common.distinct;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 4:52 PM
 */
public class DistinctRequest extends ActionRequest {

    private String table;

    private List<ResultSort> nesting;

    public DistinctRequest() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<ResultSort> getNesting() {
        return nesting;
    }

    public void setNesting(List<ResultSort> nesting) {
        this.nesting = nesting;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("filters", getFilters())
                .append("nesting", nesting)
                .toString();
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = new HashSet<>();
        if (CollectionUtils.isStringNullOrEmpty(table)) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (CollectionUtils.isListNullOrEmpty(nesting)) {
            validationErrors.add("At least one nesting parameter is required");
        } else {
            for (ResultSort resultSort : nesting) {
                if (resultSort == null) {
                    validationErrors.add("nested parameter cannot be null");
                } else {
                    if (CollectionUtils.isStringNullOrEmpty(resultSort.getField())) {
                        validationErrors.add("nested parameter cannot have null name");
                    }
                    if (resultSort.getOrder() == null) {
                        validationErrors.add("nested parameter cannot have null sorting order");
                    }
                }
            }
        }
        return validationErrors;
    }
}
