package com.flipkart.foxtrot.core.table.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import org.elasticsearch.common.Strings;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

/**
 * Created by rishabh.goyal on 05/12/15.
 */
@Singleton
public class FoxtrotTableManager implements TableManager {

    private final TableMetadataManager tableMetadataManager;
    private final TenantMetadataManager tenantMetadataManager;
    private final PipelineMetadataManager pipelineMetadataManager;
    private final QueryStore queryStore;
    private final DataStore dataStore;

    @Inject
    public FoxtrotTableManager(TableMetadataManager tableMetadataManager,
                               QueryStore queryStore,
                               DataStore dataStore,
                               TenantMetadataManager tenantMetadataManager,
                               PipelineMetadataManager pipelineMetadataManager) {
        this.tableMetadataManager = tableMetadataManager;
        this.queryStore = queryStore;
        this.dataStore = dataStore;
        this.tenantMetadataManager = tenantMetadataManager;
        this.pipelineMetadataManager = pipelineMetadataManager;
    }

    @Override
    public void save(Table table) {
        validateTableParams(table);
        if (tableMetadataManager.exists(table.getName())) {
            throw FoxtrotExceptions.createTableExistsException(table.getName());
        }
        validateTenantAndPipeline(table);
        queryStore.initializeTable(table);
        dataStore.initializeTable(table, false);
        tableMetadataManager.save(table);
    }

    @Override
    public void save(Table table,
                     boolean forceCreateTable) {
        validateTableParams(table);
        if (tableMetadataManager.exists(table.getName())) {
            throw FoxtrotExceptions.createTableExistsException(table.getName());
        }
        validateTenantAndPipeline(table);
        queryStore.initializeTable(table);
        dataStore.initializeTable(table, forceCreateTable);
        tableMetadataManager.save(table);
    }


    @Override
    public Table get(String name) {
        Table table = tableMetadataManager.get(name);
        if (table == null) {
            throw FoxtrotExceptions.createTableMissingException(name);
        }
        return table;
    }

    @Override
    public List<Table> getAll() {
        return tableMetadataManager.get();
    }

    @Override
    public void update(Table table) {
        validateTableParams(table);
        if (!tableMetadataManager.exists(table.getName())) {
            throw FoxtrotExceptions.createTableMissingException(table.getName());
        }

        Table existingTable = tableMetadataManager.get(table.getName());

        validateTenantAndPipeline(table);
        queryStore.updateTable(table.getName(), table);
        dataStore.updateTable(existingTable, table);
        tableMetadataManager.save(table);
    }

    @Override
    public void delete(String tableName) {
        // TODO Implement this once downstream implications are figured out
    }

    private void validateTenantAndPipeline(Table table) {
        if (!tenantMetadataManager.exists(table.getTenantName())) {
            throw FoxtrotExceptions.createTenantMissingException(table.getTenantName());
        }
        if (!Strings.isNullOrEmpty(table.getDefaultPipeline()) && !pipelineMetadataManager.exists(
                table.getDefaultPipeline())) {
            throw FoxtrotExceptions.createPipelineMissingException(table.getDefaultPipeline());
        }
    }

    private void validateTableParams(Table table) {
        if (table == null || table.getName() == null || table.getName()
                .trim()
                .isEmpty() || table.getTtl() <= 0) {
            throw FoxtrotExceptions.createBadRequestException(table != null
                    ? table.getName()
                    : null, "Invalid Table Params");
        }
    }
}
