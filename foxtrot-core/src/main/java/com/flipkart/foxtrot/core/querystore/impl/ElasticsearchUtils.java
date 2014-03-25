package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 3:46 PM
 */
public class ElasticsearchUtils {
    public static final String TYPE_NAME = "document";
    public static final String TABLENAME_PREFIX = "foxtrot";
    private static ObjectMapper mapper;

    public static void setMapper(ObjectMapper mapper) {
        ElasticsearchUtils.mapper = mapper;
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static String[] getIndices(final String table) {
        /*long currentTime = new Date().getTime();
        String names[] = new String[30]; //TODO::USE TABLE METADATA
        for(int i = 0 ; i < 30; i++) {
            String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(currentTime));
            names[i] = String.format("%s-%s-%s", TABLENAME_PREFIX, table, postfix);
        }*/
        return new String[]{String.format("%s-%s-*", ElasticsearchUtils.TABLENAME_PREFIX, table)};
    }

    public static String getCurrentIndex(final String table, long timestamp) {
        //TODO::THROW IF TIMESTAMP IS BEYOND TABLE META.TTL
        String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(timestamp));
        return String.format("%s-%s-%s", ElasticsearchUtils.TABLENAME_PREFIX, table, postfix);
    }
}
