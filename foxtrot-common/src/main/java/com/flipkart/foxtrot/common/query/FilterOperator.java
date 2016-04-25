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

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:22 PM
 */
public interface FilterOperator {
    //All
    String equals = "equals";
    String not_equals = "not_equals";
    String any = "any";
    String in = "in";
    String exists = "exists";
    String missing = "missing";

    //Numeric
    String less_than = "less_than";
    String less_equal = "less_equal";
    String greater_than = "greater_than";
    String greater_equal = "greater_equal";

    String between = "between";

    //String
    String contains = "contains";

    //Date time
    String last = "last";
}
