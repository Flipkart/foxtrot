package com.flipkart.foxtrot.sql.constants;

import lombok.experimental.UtilityClass;

/**
 * Created by rishabh.goyal on 17/11/14.
 */
@UtilityClass
public class Constants {
    public static final String SQL_TABLE_REGEX = "[^a-zA-Z0-9\\-_]";
    public static final String SQL_FIELD_REGEX = "[^a-zA-Z0-9.\\-_]";
    public static final String REGEX = "^\"+|\"+$";
    public static final String REPLACEMENT = "";
}
