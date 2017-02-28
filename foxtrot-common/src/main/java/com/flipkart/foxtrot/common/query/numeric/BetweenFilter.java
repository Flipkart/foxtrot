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
import com.flipkart.foxtrot.common.query.datetime.TimeWindow;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:10 AM
 */
public class BetweenFilter extends Filter {

    private boolean temporal = false;

    private Number from;

    private Number to;

    public BetweenFilter() {
        super(FilterOperator.between);
    }

    public BetweenFilter(String field, Number from, Number to, boolean temporal) {
        super(FilterOperator.between, field);
        this.from = from;
        this.to = to;
        this.temporal = temporal;
    }

    public Number getFrom() {
        return from;
    }

    public void setFrom(Number from) {
        this.from = from;
    }

    public Number getTo() {
        return to;
    }

    public void setTo(Number to) {
        this.to = to;
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BetweenFilter that = (BetweenFilter) o;

        if (!from.equals(that.from)) return false;
        if (!to.equals(that.to)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (!temporal) {
            result = 31 * result + from.hashCode();
            result = 31 * result + to.hashCode();
        } else {
            result = new TimeWindow(from.longValue(), to.longValue()).hashCode();
        }
        return result;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("from", from)
                .append("to", to)
                .toString();
    }

    @Override
    public boolean isFilterTemporal() {
        return temporal;
    }

    public void setTemporal(boolean temporal) {
        this.temporal = temporal;
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
