/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:20 PM
 */
public interface FilterVisitor<T> {

    T visit(BetweenFilter betweenFilter);

    T visit(EqualsFilter equalsFilter);

    T visit(NotEqualsFilter notEqualsFilter);

    T visit(ContainsFilter stringContainsFilterElement);

    T visit(GreaterThanFilter greaterThanFilter);

    T visit(GreaterEqualFilter greaterEqualFilter);

    T visit(LessThanFilter lessThanFilter);

    T visit(LessEqualFilter lessEqualFilter);

    T visit(AnyFilter anyFilter);

    T visit(InFilter inFilter);

    T visit(NotInFilter inFilter);

    T visit(ExistsFilter existsFilter);

    T visit(LastFilter lastFilter);

    T visit(MissingFilter missingFilter);

    T visit(WildCardFilter wildCardFilter);
}
