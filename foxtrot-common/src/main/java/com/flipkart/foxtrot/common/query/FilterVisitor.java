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
package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:20 PM
 */
public abstract class FilterVisitor<T> {

    public abstract T visit(BetweenFilter betweenFilter) throws Exception;

    public abstract T visit(EqualsFilter equalsFilter) throws Exception;

    public abstract T visit(NotEqualsFilter notEqualsFilter) throws Exception;

    public abstract T visit(ContainsFilter stringContainsFilterElement) throws Exception;

    public abstract T visit(GreaterThanFilter greaterThanFilter) throws Exception;

    public abstract T visit(GreaterEqualFilter greaterEqualFilter) throws Exception;

    public abstract T visit(LessThanFilter lessThanFilter) throws Exception;

    public abstract T visit(LessEqualFilter lessEqualFilter) throws Exception;

    public abstract T visit(AnyFilter anyFilter) throws Exception;

    public abstract T visit(InFilter inFilter) throws Exception;

    public abstract T visit(NotInFilter inFilter) throws Exception;

    public abstract T visit(ExistsFilter existsFilter) throws Exception;

    public abstract T visit(LastFilter lastFilter) throws Exception;

    public abstract T visit(MissingFilter missingFilter) throws Exception;
}
