package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.FilterEventsAction;
import com.flipkart.foxtrot.core.querystore.actions.QueryResponse;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */
public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());

    private ElasticsearchConnection connection;
    private DataStore dataStore;
    private ObjectMapper mapper;
    private QueryExecutor queryExecutor;


    public ElasticsearchQueryStore(ElasticsearchConnection connection,
                                   DataStore dataStore, QueryExecutor queryExecutor) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.mapper = ElasticsearchUtils.getMapper();
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void save(String table, Document document) throws QueryStoreException {
        try {
            dataStore.save(table, document);
            long timestamp = document.getTimestamp();
            IndexResponse response = connection.getClient()
                                                .prepareIndex()
                                                .setIndex(ElasticsearchUtils.getCurrentIndex(table, timestamp))
                                                .setType(ElasticsearchUtils.TYPE_NAME)
                                                .setId(document.getId())
                                                .setTimestamp(Long.toString(timestamp))
                                                .setSource(mapper.writeValueAsBytes(document.getData()))
                                                .setRefresh(true)
                                                .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                                                .execute()
                                                .get();
            if(response.getVersion() > 0) {
                return;
            }
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                            "Error saving documents: " + e.getMessage(), e);
        }
    }

    @Override
    public void save(String table, List<Document> documents) throws QueryStoreException {
        try {
            dataStore.save(table, documents);
            BulkRequestBuilder bulkRequestBuilder = connection.getClient().prepareBulk();
            for(Document document: documents) {
                long timestamp = document.getTimestamp();
                final String index = ElasticsearchUtils.getCurrentIndex(table, timestamp);
                IndexRequest indexRequest = new IndexRequest()
                                                    .index(index)
                                                    .type(ElasticsearchUtils.TYPE_NAME)
                                                    .id(document.getId())
                                                    .timestamp(Long.toString(timestamp))
                                                    .source(mapper.writeValueAsBytes(document.getData()));
                bulkRequestBuilder.add(indexRequest);
            }

            BulkResponse response = bulkRequestBuilder.setRefresh(true)
                                                        .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                                                        .execute()
                                                        .get();

        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                    "Error saving documents: " + e.getMessage(), e);
        }
    }

    @Override
    public Document get(String table, String id) throws QueryStoreException {
        try {
            return dataStore.get(table, id);
        } catch (DataStoreException e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    "Error getting documents: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Document> get(String table, List<String> ids) throws QueryStoreException {
        try {
            return dataStore.get(table, ids);
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    "Error getting documents: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResponse runQuery(final Query query) throws QueryStoreException {
        FilterEventsAction filterEventsAction = new FilterEventsAction(query, dataStore, connection);
        return queryExecutor.execute(filterEventsAction);
    }

    @Override
    public AsyncDataToken runQueryAsync(Query query) throws QueryStoreException {
        FilterEventsAction filterEventsAction = new FilterEventsAction(query, dataStore, connection);
        return queryExecutor.executeAsync(filterEventsAction);
    }

    @Override
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException {
        return null;
    }

    @Override
    public HistogramResponse histogram(final HistogramRequest histogramRequest) throws QueryStoreException {
        final String AGG_NAME = "histogram";
        try {
            /*if(!tableManager.exists(query.getTable())) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "There is no table called: " + query.getTable());
            }*/
            DateHistogram.Interval interval = null;
            switch (histogramRequest.getPeriod()) {
                case minutes:
                    interval = DateHistogram.Interval.MINUTE;
                    break;
                case hours:
                    interval = DateHistogram.Interval.HOUR;
                    break;
                case days:
                    interval = DateHistogram.Interval.DAY;
                    break;
            }
            SearchResponse response = connection.getClient().prepareSearch(ElasticsearchUtils.getIndices(histogramRequest.getTable()))
                    .setTypes(ElasticsearchUtils.TYPE_NAME)
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                            .genFilter(histogramRequest.getFilters())
                            .must(QueryBuilders.rangeQuery(histogramRequest.getField())
                                    .from(histogramRequest.getFrom())
                                    .to(histogramRequest.getTo()))
                    )
                    .setSize(0)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(AggregationBuilders.dateHistogram(AGG_NAME)
                            .field(histogramRequest.getField())
                            .interval(interval))
                    .execute()
                    .actionGet();
            DateHistogram dateHistogram = response.getAggregations().get(AGG_NAME);
            Collection<? extends DateHistogram.Bucket> buckets = dateHistogram.getBuckets();
            List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>(buckets.size());
            for(DateHistogram.Bucket bucket : buckets) {
                HistogramResponse.Count count = new HistogramResponse.Count(
                                                        bucket.getKeyAsNumber(), bucket.getDocCount());
                counts.add(count);
            }
            return new HistogramResponse(counts);
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.HISTOGRAM_GENERATION_ERROR,
                    "Malformed query", e);
        }
    }

    @Override
    public GroupResponse group(GroupRequest groupRequest) throws QueryStoreException {
        try {
            SearchRequestBuilder query = connection.getClient().prepareSearch(ElasticsearchUtils.getIndices(groupRequest.getTable()));
            TermsBuilder rootBuilder = null;
            TermsBuilder termsBuilder = null;
            for(String field : groupRequest.getNesting()) {
                if(null == termsBuilder) {
                    termsBuilder = AggregationBuilders.terms(field).field(field);
                }
                else {
                    termsBuilder.subAggregation(AggregationBuilders.terms(field).field(field));
                }
                if(null == rootBuilder) {
                    rootBuilder = termsBuilder;
                }
            }
            query.addAggregation(rootBuilder);
            SearchResponse response = query.execute().actionGet();
            List<String> fields = groupRequest.getNesting();
            Aggregations aggregations = response.getAggregations();
            return new GroupResponse(getMap(fields, aggregations));
        } catch (Exception e) {
            logger.error("Error running grouping: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                            "Error running group query.", e);
        }
    }

    private Map<String, Object> getMap(List<String> fields, Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size())
                                                                : new ArrayList<String>();
        Terms terms = aggregations.get(field);
        Map<String, Object> levelCount = new HashMap<String, Object>();
        for(Terms.Bucket bucket : terms.getBuckets()) {
            if(fields.size() == 1) { //TERMINAL AGG
                levelCount.put(bucket.getKey(), bucket.getDocCount());
            }
            else {
                levelCount.put(bucket.getKey(), getMap(remainingFields, bucket.getAggregations()));
            }
        }
        return levelCount;

    }

    //@Override
/*
    public JsonNode getDataForQuery(String table, String queryId) throws QueryStoreException {
        connection.getClient().prepareSearch().setIndices(getIndices(table));
        FilterBuilders.rangeFilter("xx").gt(1);
        ClusterStateResponse clusterStateResponse = connection.getClient().admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String, MappingMetaData> indexMappings = clusterStateResponse.getState().getMetaData().index(table).getMappings();
        try {
            byte data[] = mapper.writeValueAsBytes(indexMappings.get(TYPE_NAME).source().string());
            JsonSchema schema = mapper.generateJsonSchema(Document.class);

        } catch (Exception e) {
            e.printStackTrace();
        }
        */
/*
        1. Create and save json path:type mapping
        2. use path to find type
        3. Use type to convert query to proper filter

        * *//*

        return null;
    }
*/



    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-M-ddHH:m:s.S");
        Date date = simpleDateFormat.parse("2014-03-13T07:33:00.000Z");
        System.out.println(date.getTime());
    }
}
