package com.flipkart.foxtrot.sql.util;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

import static com.flipkart.foxtrot.sql.constants.Constants.REGEX;
import static com.flipkart.foxtrot.sql.constants.Constants.REPLACEMENT;

public class QueryUtils {

    private QueryUtils() {}

    public static String expressionToString(Expression expression) {
        if(expression instanceof Column) {
            return ((Column)expression).getFullyQualifiedName().replaceAll(REGEX, REPLACEMENT);
        }
        if(expression instanceof StringValue) {
            return ((StringValue)expression).getValue().replaceAll(REGEX, REPLACEMENT);
        }
        return null;
    }

    public static Number expressionToNumber(Expression expression) {
        if(expression instanceof StringValue) {
            return Long.valueOf(((StringValue)expression).getValue());
        }
        if(expression instanceof LongValue) {
            return ((LongValue)expression).getValue();
        }
        if(expression instanceof DoubleValue) {
            return ((DoubleValue)expression).getValue();
        }
        return null;
    }

}
