package com.flipkart.foxtrot.common.query;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:22 PM
 */
public interface FilterOperator {
    //All
    public static final String equals = "equals";
    public static final String not_equals = "not_equals";

    //Numeric
    public static final String less_than = "less_than";
    public static final String less_equal = "less_equal";
    public static final String greater_than = "greater_than";
    public static final String greater_equal = "greater_equal";
    public static final String between = "between";

    //String
    public static final String contains = "contains";

    //Combiner
    public static final String and = "and";
    public static final String or = "or";

}
