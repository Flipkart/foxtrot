package com.flipkart.foxtrot.core.table.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableManagerException;
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
    public void save(Table table) throws TableManagerException {
        validateTableParams(table);
        try {
            if (metadataManager.exists(table.getName())) {
                throw new TableManagerException(TableManagerException.ErrorCode.BAD_REQUEST, "table already exists");
            }
            queryStore.initializeTable(table.getName());
            dataStore.initializeTable(table);
            metadataManager.save(table);
        } catch (DataStoreException e) {
            switch (e.getErrorCode()) {
                case TABLE_NOT_FOUND:
                    throw new TableManagerException(TableManagerException.ErrorCode.BAD_REQUEST, e);
                default:
                    throw new TableManagerException(TableManagerException.ErrorCode.INTERNAL_ERROR, e);
            }
        } catch (TableManagerException e) {
            throw e;
        } catch (Exception e) {
            throw new TableManagerException(TableManagerException.ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public Table get(String name) throws TableManagerException {
        try {
            Table table = metadataManager.get(name);
            if (table == null) {
                throw new TableManagerException(TableManagerException.ErrorCode.TABLE_NOT_FOUND,
                        String.format("table does not exists table=%s", name));
            }
            return table;
        } catch (TableManagerException e) {
            throw e;
        } catch (Exception e) {
            throw new TableManagerException(TableManagerException.ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<Table> getAll() throws TableManagerException {
        try {
            return metadataManager.get();
        } catch (Exception e) {
            throw new TableManagerException(TableManagerException.ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public void update(Table table) throws TableManagerException {
        validateTableParams(table);
        try {
            if (!metadataManager.exists(table.getName())) {
                throw new TableManagerException(TableManagerException.ErrorCode.TABLE_NOT_FOUND, "table_does_not_exists");
            }
            metadataManager.save(table);
        } catch (TableManagerException e) {
            throw e;
        } catch (Exception e) {
            throw new TableManagerException(TableManagerException.ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public void delete(String tableName) throws TableManagerException {
        // TODO Implement this once downstream implications are figured out
    }

    private void validateTableParams(Table table) throws TableManagerException {
        if (table == null || table.getName() == null || table.getName().trim().isEmpty() || table.getTtl() <= 0) {
            throw new TableManagerException(TableManagerException.ErrorCode.BAD_REQUEST, "Invalid Table Params");
        }
    }
}
