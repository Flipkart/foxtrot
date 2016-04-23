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
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.util.TableUtil;
import io.dropwizard.lifecycle.Managed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:35 PM
 */
public class HbaseTableConnection implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(HbaseTableConnection.class.getSimpleName());

    private final HbaseConfig hbaseConfig;
    private Connection connection;
    private Admin hBaseAdmin;

    public HbaseTableConnection(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public synchronized org.apache.hadoop.hbase.client.Table getTable(final Table table) throws FoxtrotException {
        try {
            if (hbaseConfig.isSecure() && UserGroupInformation.isSecurityEnabled()) {
                UserGroupInformation.getCurrentUser().reloginFromKeytab();
            }
            return connection.getTable(TableName.valueOf(TableUtil.getTableName(hbaseConfig, table)));
        } catch (Exception e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
    }

    public synchronized boolean isTableAvailable(final Table table) throws IOException {
        String tableName = TableUtil.getTableName(hbaseConfig, table);
        return hBaseAdmin.isTableAvailable(TableName.valueOf(tableName));
    }

    public String getHBaseTableName(final Table table) throws IOException {
        return TableUtil.getTableName(hbaseConfig, table);
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting HBase Connection");
        Configuration configuration = HBaseUtil.create(hbaseConfig);
        connection = ConnectionFactory.createConnection(configuration);
        this.hBaseAdmin = connection.getAdmin();
        logger.info("Started HBase Connection");
    }

    @Override
    public void stop() throws Exception {
        connection.close();
        hBaseAdmin.close();
    }

    public HbaseConfig getHbaseConfig() {
        return hbaseConfig;
    }
}
