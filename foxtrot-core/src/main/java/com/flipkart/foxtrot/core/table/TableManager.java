package com.flipkart.foxtrot.core.table;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.exception.FoxtrotException;

import java.util.List;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
public interface TableManager {

    void save(Table table) throws FoxtrotException;

    Table get(String name) throws FoxtrotException;

    List<Table> getAll() throws FoxtrotException;

    void update(Table table) throws FoxtrotException;

    void delete(String name) throws FoxtrotException;

}
