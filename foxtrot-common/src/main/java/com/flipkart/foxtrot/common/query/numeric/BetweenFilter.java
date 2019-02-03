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
 * Time: 2:10 AM
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Data
public class BetweenFilter extends Filter {

    private boolean temporal;

    @NotNull
    private Number from;

    @NotNull
    private Number to;

    public BetweenFilter() {
        super(FilterOperator.between);
    }

    @Builder
    public BetweenFilter(String field, Number from, Number to, boolean temporal) {
        super(FilterOperator.between, field);
        this.from = from;
        this.to = to;
        this.temporal = temporal;
    }

    @Override
    public<T> T accept(FilterVisitor<T> visitor) throws Exception {
        return visitor.visit(this);
    }

    @Override
    public boolean isFilterTemporal() {
        return temporal;
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = super.validate();
        if (from == null) {
            validationErrors.add("from field cannot be null");
        }

        if (to == null) {
            validationErrors.add("to field cannot be null");
        }
        return validationErrors;
    }
}
