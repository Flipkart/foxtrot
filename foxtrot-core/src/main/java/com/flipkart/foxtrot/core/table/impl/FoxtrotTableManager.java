package com.flipkart.foxtrot.core.table.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;

import java.util.List;

/**
 * Created by rishabh.goyal on 05/12/15.
 */

public class FoxtrotTableManager implements TableManager {

    private final TableMetadataManager metadataManager;
    private final QueryStore queryStore;
    private final DataStore dataStore;

    public FoxtrotTableManager(TableMetadataManager metadataManager, QueryStore queryStore, DataStore dataStore) {
        this.metadataManager = metadataManager;
        this.queryStore = queryStore;
        this.dataStore = dataStore;
    }


    @Override
    public void save(Table table) throws FoxtrotException {
        validateTableParams(table);
        if (metadataManager.exists(table.getName())) {
            throw FoxtrotExceptions.createTableExistsException(table.getName());
        }
        queryStore.initializeTable(table.getName());
        dataStore.initializeTable(table);
        metadataManager.save(table);
    }

    @Override
    public Table get(String name) throws FoxtrotException {
        Table table = metadataManager.get(name);
        if (table == null) {
            throw FoxtrotExceptions.createTableMissingException(name);
        }
        return table;
    }

    @Override
    public List<Table> getAll() throws FoxtrotException {
        return metadataManager.get();
    }

    @Override
    public void update(Table table) throws FoxtrotException {
        validateTableParams(table);
        if (!metadataManager.exists(table.getName())) {
            throw FoxtrotExceptions.createTableMissingException(table.getName());
        }
        metadataManager.save(table);
    }

    @Override
    public void delete(String tableName) throws FoxtrotException {
        // TODO Implement this once downstream implications are figured out
    }

    private void validateTableParams(Table table) throws FoxtrotException {
        if (table == null || table.getName() == null || table.getName().trim().isEmpty() || table.getTtl() <= 0) {
            throw FoxtrotExceptions.createBadRequestException(table != null ? table.getName() : null, "Invalid Table Params");
        }
    }
}
