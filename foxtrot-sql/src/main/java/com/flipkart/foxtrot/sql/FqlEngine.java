package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.flipkart.foxtrot.sql.query.FqlDescribeTable;
import com.flipkart.foxtrot.sql.query.FqlShowTablesQuery;
import com.flipkart.foxtrot.sql.responseprocessors.Flattener;
import com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FqlEngine {
    private static final Logger logger = LoggerFactory.getLogger(FqlEngine.class.getSimpleName());

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
        FlatRepresentation response = new QueryProcessor(tableMetadataManager, queryStore, queryExecutor, mapper).process(query);
        logger.debug("Flat Response: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(response));
        return response;
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
            logger.info("Generated query: " + mapper.writeValueAsString(fqlActionQuery.getActionRequest()));
            ActionResponse actionResponse = queryExecutor.execute(fqlActionQuery.getActionRequest());
            Flattener flattener = new Flattener(mapper, fqlActionQuery.getActionRequest(), fqlActionQuery.getSelectedFields());
            actionResponse.accept(flattener);
            result = flattener.getFlatRepresentation();
        }

    }
}
