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
package com.flipkart.foxtrot.common.query.numeric;

import com.flipkart.foxtrot.common.query.Filter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:25 PM
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public abstract class NumericBinaryFilter extends Filter {

    @NotNull
    private Number value;

    private boolean temporal = false;

    protected NumericBinaryFilter(final String operator) {
        super(operator);
    }

    protected NumericBinaryFilter(final String operator, String field, Number value, boolean temporal) {
        super(operator, field);
        this.value = value;
        this.temporal = temporal;
    }

    @Override
    public boolean isFilterTemporal() {
        return temporal;
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
