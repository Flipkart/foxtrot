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
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.util.TableUtil;
import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:35 PM
 */
@CoverageIgnore
public class HbaseTableConnection implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(HbaseTableConnection.class.getSimpleName());

    private final HbaseConfig hbaseConfig;
    private HTablePool tablePool;

    public HbaseTableConnection(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }


    public synchronized HTableInterface getTable(final Table table) throws DataStoreException {
        try {
            if (hbaseConfig.isSecure() && UserGroupInformation.isSecurityEnabled()) {
                UserGroupInformation.getCurrentUser().reloginFromKeytab();
            }
            return tablePool.getTable(TableUtil.getTableName(hbaseConfig, table));
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_CONNECTION,
                    t.getMessage(), t);
        }
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting HBase Connection");
        Configuration configuration = HBaseUtil.create(hbaseConfig);
        tablePool = new HTablePool(configuration, 10, PoolMap.PoolType.Reusable);
        logger.info("Started HBase Connection");
    }

    @Override
    public void stop() throws Exception {
        tablePool.close();
    }
}
