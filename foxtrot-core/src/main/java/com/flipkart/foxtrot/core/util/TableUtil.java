package com.flipkart.foxtrot.core.util;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.google.common.base.Strings;

public class TableUtil {
    private TableUtil() {

    }

    public static String getTableName(final HbaseConfig hbaseConfig, final Table table) {
        if(table.isSeggregatedBackend()) {
            final String tablePrefix = hbaseConfig.getSeggregatedTablePrefix();
            if( !Strings.isNullOrEmpty(tablePrefix)) {
                return String.format("%s%s", tablePrefix, table.getName());
            }
            else {
                return table.getName();
            }
        }
        return hbaseConfig.getTableName();
    }
}
