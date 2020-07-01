package com.flipkart.foxtrot.sql.constants;

public class FqlFunctionType {

    private FqlFunctionType(){
        throw new IllegalStateException("Utility Class");
    }

    public static final String TREND = "trend";
    public static final String STATSTREND = "statstrend";
    public static final String STATS = "stats";
    public static final String HISTOGRAM = "histogram";
    public static final String COUNT = "count";
    public static final String SUM = "sum";
    public static final String AVG = "avg";
    public static final String MIN = "min";
    public static final String MAX = "max";

}
