package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Table;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:08 PM
 */
public interface TableManager {
    public void save(Table table) throws Exception;
    public Table get(String tableName) throws Exception;
    public boolean exists(String tableName) throws Exception;
}
