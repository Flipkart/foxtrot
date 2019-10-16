/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.commons.lang.builder.ToStringBuilder;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:10 AM
 */
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
    public <T> T accept(FilterVisitor<T> visitor) {
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

    @Override
    public int hashCode() {
        int result = getOperator().hashCode();
        result = 31 * result + getField().hashCode();
        if (!getField().equals("_timestamp")) {
            result = result * 21 + (getFrom() == null ? 43 : getFrom().hashCode());
            result = result * 23 + (getTo() == null ? 29 : getTo().hashCode());
        } else {
            result = result * 21 + Long.valueOf(getFrom().longValue() / (long)30000).hashCode();
            result = result * 23 + Long.valueOf(getTo().longValue() / (long)30000).hashCode();
        }
        result = result * 59 + (this.isTemporal() ? 79 : 97);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof BetweenFilter)) {
            return false;
        }

        BetweenFilter that = (BetweenFilter) o;

        return getField().equals(that.getField()) && getOperator().equals(that.getOperator()) &&
                isFilterTemporal() == that.isFilterTemporal() && getFrom().equals(that.getFrom()) &&
                getTo().equals(that.getTo());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("temporal", temporal)
                .append("from", from.toString())
                .append("to", to.toString())
                .toString();
    }

}
