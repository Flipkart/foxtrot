package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.flipkart.foxtrot.core.util.OpensearchQueryUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import lombok.Data;
import org.joda.time.DateTime;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created By vashu.shivam on 07/09/23
 */
@Data
@Singleton
public class OpensearchQueryStore implements QueryStore {

    private static final Logger logger = LoggerFactory.getLogger(OpensearchQueryStore.class.getSimpleName());
    private static final String TABLE_META = "tableMeta";
    private static final String DATA_STORE = "dataStore";
    private static final String QUERY_STORE = "queryStore";
    private static final String UNKNOWN_TABLE_ERROR_MESSAGE = "unknown_table table:%s";

    private final OpensearchConnection connection;
    private final DataStore dataStore;
    private final TableMetadataManager tableMetadataManager;
    private final List<IndexerEventMutator> mutators;
    private final ObjectMapper mapper;
    private final CardinalityConfig cardinalityConfig;

    public OpensearchQueryStore(TableMetadataManager tableMetadataManager,
                                OpensearchConnection connection,
                                DataStore dataStore,
                                List<IndexerEventMutator> mutators,
                                ObjectMapper mapper,
                                CardinalityConfig cardinalityConfig) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.tableMetadataManager = tableMetadataManager;
        this.mutators = mutators;
        this.mapper = mapper;
        this.cardinalityConfig = cardinalityConfig;
    }

    @Override
    public void initializeTable(String table) {
        //Do Nothing
    }

    @Override
    public void save(String table,
                     Document document) {
        table = OpensearchUtils.getValidTableName(table);
        Stopwatch stopwatch = Stopwatch.createStarted();
        String action = null;
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table,
                        String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
            }
            if (new DateTime().plusDays(1)
                    .minus(document.getTimestamp())
                    .getMillis() < 0) {
                return;
            }
            action = TABLE_META;
            stopwatch.reset()
                    .start();
            final Table tableMeta = tableMetadataManager.get(table);
            logger.debug("TableMetaGetTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = DATA_STORE;
            final Document translatedDocument = dataStore.save(tableMeta, document);
            logger.debug("DataStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = QUERY_STORE;
            long timestamp = translatedDocument.getTimestamp();
            IndexRequest indexRequest = new IndexRequest(OpensearchUtils.getCurrentIndex(table, timestamp)).id(
                            translatedDocument.getId())
                    .source(convert(table, translatedDocument))
                    .timeout(new TimeValue(2, TimeUnit.SECONDS));
            getConnection().getClient()
                    .index(indexRequest, RequestOptions.DEFAULT);
            logger.debug("QueryStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (IOException e) {
            MetricUtil.getInstance()
                    .registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            Thread.currentThread()
                    .interrupt();
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    public void save(String table,
                     List<Document> documents) {

    }

    @Override
    public Document get(String table,
                        String id) {
        return null;
    }

    @Override
    public List<Document> getAll(String table,
                                 List<String> ids) {
        return null;
    }

    @Override
    public List<Document> getAll(String table,
                                 List<String> ids,
                                 boolean bypassMetaLookup) {
        return null;
    }

    @Override
    public void cleanupAll() {

    }

    @Override
    public void cleanup(String table) {

    }

    @Override
    public void cleanup(Set<String> tables) {

    }

    @Override
    public ClusterHealthResponse getClusterHealth() {
        return null;
    }

    @Override
    public JsonNode getNodeStats() {
        return null;
    }

    @Override
    public JsonNode getIndicesStats() throws IOException {
        return null;
    }

    @Override
    public TableFieldMapping getFieldMappings(String table) {
        return null;
    }

    private Map<String, Object> convert(String table,
                                        Document document) {
        JsonNode metaNode = mapper.valueToTree(document.getMetadata());
        ObjectNode dataNode = document.getData()
                .deepCopy();
        dataNode.set(OpensearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        dataNode.set(OpensearchUtils.DOCUMENT_TIME_FIELD_NAME, mapper.valueToTree(document.getDate()));
        mutators.forEach(mutator -> mutator.mutate(table, document.getId(), dataNode));
        return OpensearchQueryUtils.toMap(mapper, dataNode);
    }

    private void deleteIndices(List<String> indicesToDelete) {
        logger.warn("Deleting Indexes - Indexes - {}", indicesToDelete);
        if (!indicesToDelete.isEmpty()) {
            List<List<String>> subLists = Lists.partition(indicesToDelete, 5);
            for (List<String> subList : subLists) {
                try {
                    connection.getClient()
                            .indices()
                            .delete(new DeleteIndexRequest(subList.toArray(new String[0])), RequestOptions.DEFAULT);
                    logger.warn("Deleted Indexes - Indexes - {}", subList);
                } catch (Exception e) {
                    logger.error("Index deletion failed - Indexes - {}", subList, e);
                }
            }
        }
    }

    private List<String> getIndicesToDelete(Set<String> tables,
                                            Collection<String> currentIndices) {
        List<String> indicesToDelete = new ArrayList<>();
        for (String currentIndex : currentIndices) {
            String table = OpensearchUtils.getTableNameFromIndex(currentIndex);
            if (table != null && tables.contains(table)) {
                boolean indexEligibleForDeletion;
                try {
                    indexEligibleForDeletion = OpensearchUtils.isIndexEligibleForDeletion(currentIndex,
                            tableMetadataManager.get(table));
                    if (indexEligibleForDeletion) {
                        logger.warn("Index eligible for deletion : {}", currentIndex);
                        indicesToDelete.add(currentIndex);
                    }
                } catch (Exception ex) {
                    logger.error("Unable to Get Table details for Table : {}", table, ex);
                }
            }
        }
        return indicesToDelete;
    }
}
