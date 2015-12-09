package com.flipkart.foxtrot.core.table;

import com.flipkart.foxtrot.common.Table;

import java.util.List;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
public interface TableManager {

    void save(Table table) throws TableManagerException;

    Table get(String name) throws TableManagerException;

    List<Table> getAll() throws TableManagerException;

    void update(Table table) throws TableManagerException;

    void delete(String name) throws TableManagerException;

}
