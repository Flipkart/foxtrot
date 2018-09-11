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
package com.flipkart.foxtrot.common.query.string;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 3:46 PM
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ContainsFilter extends Filter {

    private String value;

    public ContainsFilter() {
        super(FilterOperator.contains);
    }

    @Builder
    public ContainsFilter(String value) {
        super(FilterOperator.contains);
        this.value = value;
    }

    @Override
    public<T> T accept(FilterVisitor<T> visitor) throws Exception {
        return visitor.visit(this);
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = super.validate();
        if (value == null) {
            validationErrors.add("filter value cannot be null");
        }
        return validationErrors;
    }
}
