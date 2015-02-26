/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
