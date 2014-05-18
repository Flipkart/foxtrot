package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

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


    public synchronized HTableInterface getTable() throws DataStoreException {
        try {
            if (hbaseConfig.isSecure() && UserGroupInformation.isSecurityEnabled()) {
                UserGroupInformation.getCurrentUser().reloginFromKeytab();
            }
            return tablePool.getTable(hbaseConfig.getTableName());
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_CONNECTION,
                    t.getMessage(), t);
        }
    }

    @Override
    public void start() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        if(null != hbaseConfig.getKeytabFileName() && !hbaseConfig.getKeytabFileName().isEmpty()) {
            File file = new File(hbaseConfig.getKeytabFileName());
            if (file.exists()) {
                configuration.addResource(new File(hbaseConfig.getCoreSite()).toURI().toURL());
                configuration.addResource(new File(hbaseConfig.getHdfsSite()).toURI().toURL());
                configuration.addResource(new File(hbaseConfig.getHbasePolicy()).toURI().toURL());
                configuration.addResource(new File(hbaseConfig.getHbaseSite()).toURI().toURL());
                configuration.set("hbase.master.kerberos.principal", hbaseConfig.getAuthString());
                configuration.set("hadoop.kerberos.kinit.command", hbaseConfig.getKinitPath());
                UserGroupInformation.setConfiguration(configuration);
                System.setProperty("java.security.krb5.conf", hbaseConfig.getKerberosConfigFile());
                if (hbaseConfig.isSecure()) {
                    UserGroupInformation.loginUserFromKeytab(
                            hbaseConfig.getAuthString(), hbaseConfig.getKeytabFileName());
                    logger.info("Logged into Hbase with User: " + UserGroupInformation.getLoginUser());
                }
            }
        }
        tablePool = new HTablePool(configuration, 10, PoolMap.PoolType.Reusable);
    }

    @Override
    public void stop() throws Exception {
        tablePool.close();
    }
}
