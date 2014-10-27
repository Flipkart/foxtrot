package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.flipkart.foxtrot.sql.query.FqlDescribeTable;
import com.flipkart.foxtrot.sql.query.FqlShowTablesQuery;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.flipkart.foxtrot.sql.responseprocessors.Flattener;
import com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils;
import com.google.common.collect.Lists;

import java.util.List;

public class FqlEngine {
    private TableMetadataManager tableMetadataManager;
    private QueryStore queryStore;
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper;

    public FqlEngine(TableMetadataManager tableMetadataManager, QueryStore queryStore, QueryExecutor queryExecutor, ObjectMapper mapper) {
        this.tableMetadataManager = tableMetadataManager;
        this.queryStore = queryStore;
        this.queryExecutor = queryExecutor;
        this.mapper = mapper;
    }

    public FlatRepresentation parse(final String fql) throws Exception {
        QueryTranslator translator = new QueryTranslator();
        FqlQuery query = translator.translate(fql);
        return new QueryProcessor(tableMetadataManager, queryStore, queryExecutor, mapper).process(query);
    }

    private static final class QueryProcessor implements FqlQueryVisitor {
        private TableMetadataManager tableMetadataManager;
        private QueryStore queryStore;
        private QueryExecutor queryExecutor;
        private ObjectMapper mapper;

        private FlatRepresentation result;

        private QueryProcessor(TableMetadataManager tableMetadataManager, QueryStore queryStore, QueryExecutor queryExecutor, ObjectMapper mapper) {
            this.tableMetadataManager = tableMetadataManager;
            this.queryStore = queryStore;
            this.queryExecutor = queryExecutor;
            this.mapper = mapper;
        }

        public FlatRepresentation process(FqlQuery query) throws Exception {
            query.receive(this);
            return result;
        }

        @Override
        public void visit(FqlDescribeTable fqlDescribeTable) throws Exception {
            TableFieldMapping fieldMetaData = queryStore.getFieldMappings(fqlDescribeTable.getTableName());
            result = FlatteningUtils.genericMultiRowParse(
                                                mapper.valueToTree(fieldMetaData.getMappings()),
                                                Lists.newArrayList("field", "type"), "field");
        }

        @Override
        public void visit(FqlShowTablesQuery fqlShowTablesQuery) throws Exception {
            List<Table> tables = tableMetadataManager.get();
            result = FlatteningUtils.genericMultiRowParse(
                    mapper.valueToTree(tables),
                    Lists.newArrayList("name", "ttl"), "name");
        }

        @Override
        public void visit(FqlActionQuery fqlActionQuery) throws Exception {
            ActionResponse actionResponse = queryExecutor.execute(fqlActionQuery.getActionRequest());
            Flattener flattener = new Flattener(mapper, fqlActionQuery.getActionRequest(), fqlActionQuery.getSelectedFields());
            actionResponse.accept(flattener);
            result = flattener.getFlatRepresentation();
        }

    }
}
