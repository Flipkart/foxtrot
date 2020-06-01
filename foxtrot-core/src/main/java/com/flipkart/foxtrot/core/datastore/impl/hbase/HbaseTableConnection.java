/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.util.TableUtil;
import io.dropwizard.lifecycle.Managed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:35 PM
 */
@Singleton
@Order(0)
public class HbaseTableConnection implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(HbaseTableConnection.class.getSimpleName());
    private static final String DEFAULT_FAMILY_NAME = "d";

    private final HbaseConfig hbaseConfig;
    private Configuration configuration;
    private Connection connection;
    private Admin hBaseAdmin;

    @Inject
    public HbaseTableConnection(Configuration configuration, HbaseConfig hbaseConfig) {
        this.configuration = configuration;
        this.hbaseConfig = hbaseConfig;
    }

    public synchronized org.apache.hadoop.hbase.client.Table getTable(final Table table) {
        try {
            if(hbaseConfig.isSecure() && UserGroupInformation.isSecurityEnabled()) {
                UserGroupInformation.getCurrentUser()
                        .reloginFromKeytab();
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

    public synchronized void createTable(final Table table) throws IOException {
        HTableDescriptor hTableDescriptor = constructHTableDescriptor(table);
        byte[][] splits = new RegionSplitter.HexStringSplit().split(table.getDefaultRegions());
        hBaseAdmin.createTable(hTableDescriptor, splits);
    }

    public synchronized void updateTable(final Table table) throws IOException {
        String tableName = TableUtil.getTableName(hbaseConfig, table);

        HTableDescriptor hTableDescriptor = constructHTableDescriptor(table);
        hBaseAdmin.modifyTable(TableName.valueOf(tableName), hTableDescriptor);
    }

    public String getHBaseTableName(final Table table) {
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

    private HTableDescriptor constructHTableDescriptor(final Table table) {
        String tableName = TableUtil.getTableName(hbaseConfig, table);

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY_NAME);
        hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
        hColumnDescriptor.setTimeToLive(Math.toIntExact(TimeUnit.DAYS.toSeconds(table.getTtl())));
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }
}
