package com.flipkart.foxtrot.core.querystore.actions;

import java.util.regex.Pattern;

/**
 * Created by rishabh.goyal on 14/05/14.
 */
public class ActionConstants {
    public static final String AGGREGATION_FIELD_REPLACEMENT_REGEX = "\\.";
    public static final String AGGREGATION_FIELD_REPLACEMENT_VALUE = "_";
    public static final Pattern VALID_AGG_NAME = Pattern.compile("[a-zA-Z0-9\\-_]+");

}
