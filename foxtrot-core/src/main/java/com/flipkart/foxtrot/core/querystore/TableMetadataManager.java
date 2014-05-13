package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Table;
import com.yammer.dropwizard.lifecycle.Managed;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:08 PM
 */
public interface TableMetadataManager extends Managed {
    public void save(Table table) throws Exception;
    public Table get(String tableName) throws Exception;
    public List<Table> get() throws Exception;
    public boolean exists(String tableName) throws Exception;
}
