/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.google.common.base.Strings;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

@UtilityClass
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    public static void createTable(final HbaseConfig hbaseConfig, final String tableName) throws IOException {
        final TableName name = TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("d");
        columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
        hTableDescriptor.addFamily(columnDescriptor);
        Configuration configuration = HBaseUtil.create(hbaseConfig);
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            try (Admin admin = connection.getAdmin()) {
                if (admin.tableExists(TableName.valueOf(tableName))) {
                    logger.info("Table {} already exists. Nothing to do.", tableName);
                    return;
                }
                logger.info("Creating table: {}", tableName);
                admin.createTable(hTableDescriptor);
            } catch (Exception e) {
                logger.error("Could not create table: " + tableName, e);
            }
        }
    }

    public static Configuration create(final HbaseConfig hbaseConfig) throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        if (isValidFile(hbaseConfig.getCoreSite())) {
            configuration.addResource(new File(hbaseConfig.getCoreSite()).toURI()
                    .toURL());
        }

        if (isValidFile(hbaseConfig.getHdfsSite())) {
            configuration.addResource(new File(hbaseConfig.getHdfsSite()).toURI()
                    .toURL());
        }

        if (isValidFile(hbaseConfig.getHbasePolicy())) {
            configuration.addResource(new File(hbaseConfig.getHbasePolicy()).toURI()
                    .toURL());
        }

        if (isValidFile(hbaseConfig.getHbaseSite())) {
            configuration.addResource(new File(hbaseConfig.getHbaseSite()).toURI()
                    .toURL());
        }

        if (hbaseConfig.isSecure() && isValidFile(hbaseConfig.getKeytabFileName())) {
            configuration.set("hbase.master.kerberos.principal", hbaseConfig.getAuthString());
            configuration.set("hadoop.kerberos.kinit.command", hbaseConfig.getKinitPath());
            UserGroupInformation.setConfiguration(configuration);
            System.setProperty("java.security.krb5.conf", hbaseConfig.getKerberosConfigFile());
            UserGroupInformation.loginUserFromKeytab(hbaseConfig.getAuthString(), hbaseConfig.getKeytabFileName());
            logger.info("Logged into Hbase with User: {}", UserGroupInformation.getLoginUser());
        }

        if (null != hbaseConfig.getHbaseZookeeperQuorum()) {
            configuration.set("hbase.zookeeper.quorum", hbaseConfig.getHbaseZookeeperQuorum());
        }

        if (!Strings.isNullOrEmpty(hbaseConfig.getHbaseZookeeperZnodeParent())) {
            configuration.set("zookeeper.znode.parent", hbaseConfig.getHbaseZookeeperZnodeParent());
        }

        if (null != hbaseConfig.getHbaseZookeeperClientPort()) {
            configuration.setInt("hbase.zookeeper.property.clientPort", hbaseConfig.getHbaseZookeeperClientPort());
        }
        return configuration;
    }

    public static boolean isValidFile(String fileName) {
        return fileName != null && !fileName.trim()
                .isEmpty() && new File(fileName).exists();
    }
}
