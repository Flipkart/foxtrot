package com.flipkart.foxtrot.sql.util;

import com.flipkart.foxtrot.sql.constants.Constants;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class QueryUtils {

    private QueryUtils() {
    }

    public static String expressionToString(Expression expression) {
        if (expression instanceof Column) {
            return ((Column) expression).getFullyQualifiedName()
                    .replaceAll(Constants.REGEX, Constants.REPLACEMENT);
        }
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue()
                    .replaceAll(Constants.REGEX, Constants.REPLACEMENT);
        }
        return null;
    }

    public static Number expressionToNumber(Expression expression) {
        if (expression instanceof StringValue) {
            return Long.valueOf(((StringValue) expression).getValue());
        }
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        return null;
    }

}
