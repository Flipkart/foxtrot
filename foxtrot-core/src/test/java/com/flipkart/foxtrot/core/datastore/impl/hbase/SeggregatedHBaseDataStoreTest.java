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
package com.flipkart.foxtrot.core.datastore.impl.hbase;


import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.util.TableUtil;
import org.junit.Assert;
import org.junit.Test;

public class SeggregatedHBaseDataStoreTest {
    @Test
    public void testSameTable() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setTableName("foxtrot-test");
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 7, false);
        Assert.assertEquals(hbaseConfig.getTableName(), TableUtil.getTableName(hbaseConfig, table));
    }

    @Test
    public void testSegTable() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setTableName("foxtrot-test");
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 7, true);
        Assert.assertEquals(TestUtils.TEST_TABLE_NAME, TableUtil.getTableName(hbaseConfig, table));
    }

    @Test
    public void testPrefixedSegTable() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setTableName("foxtrot-test");
        hbaseConfig.setSeggregatedTablePrefix("foxtrot-");
        Table table = new Table(TestUtils.TEST_TABLE_NAME, 7, true);
        Assert.assertEquals(String.format("%s%s", hbaseConfig.getSeggregatedTablePrefix(), TestUtils.TEST_TABLE_NAME),
                            TableUtil.getTableName(hbaseConfig, table));
    }
}
