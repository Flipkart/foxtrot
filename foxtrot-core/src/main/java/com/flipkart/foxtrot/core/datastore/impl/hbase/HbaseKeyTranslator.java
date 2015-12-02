package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.shash.hbase.ds.AbstractRowKeyDistributor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by santanu.s on 02/12/15.
 */
public class HbaseKeyTranslator {
    private final AbstractRowKeyDistributor keyDistributor;

    public HbaseKeyTranslator(AbstractRowKeyDistributor keyDistributor) {
        this.keyDistributor = keyDistributor;
    }

    public byte[] idToRowKey(String id) {
        return keyDistributor.getDistributedKey(Bytes.toBytes(id));
    }
}
