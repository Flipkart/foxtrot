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

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:22 PM
 */
public final class FilterOperator {

    //All
    public static final String equals = "equals";
    public static final String not_equals = "not_equals";
    public static final String any = "any";
    public static final String in = "in";
    public static final String not_in = "not_in";
    public static final String exists = "exists";
    public static final String missing = "missing";
    //Numeric
    public static final String less_than = "less_than";
    public static final String less_equal = "less_equal";
    public static final String greater_than = "greater_than";
    public static final String greater_equal = "greater_equal";
    public static final String between = "between";
    //String
    public static final String contains = "contains";
    public static final String wildcard = "wildcard";
    //Date time
    public static final String last = "last";

    private FilterOperator() {
    }
}
