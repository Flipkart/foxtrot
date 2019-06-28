package com.flipkart.foxtrot.core.table;

import com.flipkart.foxtrot.common.Table;

import java.util.List;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
public interface TableManager {

    void save(Table table);

    void save(Table table, boolean forceCreateTable);

    Table get(String name);

    List<Table> getAll();

    void update(Table table);

    void delete(String name);

}
