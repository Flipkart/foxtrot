package com.flipkart.foxtrot.core.datastore.impl.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;

import java.util.List;

@Slf4j
public class HbaseRegions {

    private Connection connection;
    private Admin hBaseAdmin;

    public HbaseRegions(HbaseConfig hbaseConfig) {
        try {
            Configuration configuration = HBaseUtil.create(hbaseConfig);
            connection = ConnectionFactory.createConnection(configuration);
            this.hBaseAdmin = connection.getAdmin();
        } catch (Exception e) {
            log.info("Unable to get Hbase Admin!!!", e);
        }
    }

    public List<HRegionInfo> getRegions(TableName tablename) {
        try {
//            RegionSizeCalculator
            return hBaseAdmin.getTableRegions(tablename);
        } catch (Exception e) {
            log.info("Unable to retrieve regions for given tablename", e);
            return null;
        }
    }
}
