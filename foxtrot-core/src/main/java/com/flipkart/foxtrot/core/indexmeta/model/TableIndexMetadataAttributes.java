package com.flipkart.foxtrot.core.indexmeta.model;

public final class TableIndexMetadataAttributes {

    public static final String TABLE = "table";
    public static final String INDEX_NAME = "indexName";
    public static final String DATE_POST_FIX = "datePostFix";
    public static final String SHARD_COUNT = "shardCount";
    public static final String NO_OF_COLUMNS = "noOfColumns";
    public static final String NO_OF_EVENTS = "noOfEvents";
    public static final String TIMESTAMP = "timestamp";
    public static final String UPDATED_AT = "updatedAt";

    private TableIndexMetadataAttributes() {
        throw new IllegalStateException("Utility class");
    }


}
