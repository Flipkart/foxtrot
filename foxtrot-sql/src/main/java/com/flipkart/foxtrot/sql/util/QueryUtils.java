package com.flipkart.foxtrot.sql.util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class QueryUtils {
    public static String expressionToString(Expression expression) {
        if(expression instanceof Column) {
            return ((Column)expression).getFullyQualifiedName();
        }
        if(expression instanceof StringValue) {
            return ((StringValue)expression).getValue();
        }
        return null;
    }


}
