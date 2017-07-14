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
package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 3:35 PM
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class NotEqualsFilter extends Filter {

    @NotNull
    private Object value;

    public NotEqualsFilter() {
        super(FilterOperator.not_equals);
    }

    @Builder
    public NotEqualsFilter(String field, String value) {
        super(FilterOperator.not_equals, field);
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
            validationErrors.add("filter field value cannot be null or empty");
        }
        return validationErrors;
    }
}
