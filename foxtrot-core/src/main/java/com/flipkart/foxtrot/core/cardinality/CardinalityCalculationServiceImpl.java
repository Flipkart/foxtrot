package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.estimation.*;
import com.flipkart.foxtrot.common.exception.CardinalityCalculationException;
import com.flipkart.foxtrot.common.exception.CardinalityMapStoreException;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfoSummary.CardinalityCalculationAuditInfoSummaryBuilder;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfoSummary.CountSummary;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfoSummary.CountSummary.CountSummaryBuilder;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfoSummary.TimeTakenSummary;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfoSummary.TimeTakenSummary.TimeTakenSummaryBuilder;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.common.util.MapUtils.mapSize;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.STATUS_ATTRIBUTE;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.TIME_TAKEN_ATTRIBUTE;

@Slf4j
@Singleton
public class CardinalityCalculationServiceImpl implements CardinalityCalculationService {

    public static final String CARDINALITY_AUDIT_INFO_INDEX = "table_cardinality_audit";
    private static final String CARDINALITY = "cardinality";
    private static final String COUNT_AGGREGATION = "statusCount";
    private static final String TIME_TAKEN_PERCENTILE_AGGREGATION = "timeTakenPercentiles";
    private static final String TIME_TAKEN_STATS_AGGREGATION = "timeTakenStats";

    private static final int PRECISION_THRESHOLD = 100;
    private final ElasticsearchConnection elasticsearchConnection;
    private int subListSize;

    @Inject
    public CardinalityCalculationServiceImpl(CardinalityConfig cardinalityConfig,
                                             ElasticsearchConnection elasticsearchConnection) {
        if (cardinalityConfig == null || cardinalityConfig.getSubListSize() == 0) {
            this.subListSize = ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE;
        } else {
            this.subListSize = cardinalityConfig.getSubListSize();
        }

        this.elasticsearchConnection = elasticsearchConnection;


    }

    @Override
    public CardinalityCalculationResult calculateCardinality(TableFieldMapping tableFieldMapping) {
        List<Exception> exceptions = new ArrayList<>();

        estimateCardinality(tableFieldMapping.getTable(), tableFieldMapping.getMappings(), DateTime.now()
                .minusDays(1)
                .toDate()
                .getTime(), exceptions);

        return CardinalityCalculationResult.builder()
                .errors(exceptions)
                .tableFieldMapping(TableFieldMapping.builder()
                        .table(tableFieldMapping.getTable())
                        .mappings(tableFieldMapping.getMappings()
                                .stream()
                                .map(fieldMetadata -> FieldMetadata.builder()
                                        .field(fieldMetadata.getField())
                                        .type(fieldMetadata.getType())
                                        .estimationData(fieldMetadata.getEstimationData())
                                        .build())
                                .collect(Collectors.toSet()))
                        .build())
                .build();
    }

    @Override
    public void updateAuditInfo(String table,
                                CardinalityCalculationAuditInfo auditInfo) {
        try {
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(CARDINALITY_AUDIT_INFO_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                            table).timeout(new TimeValue(2, TimeUnit.SECONDS))
                            .source(JsonUtils.toBytes(auditInfo), XContentType.JSON), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("Error in saving cardinality audit info for table:{} error message: {}", table, e.getMessage(),
                    e);
            throw new CardinalityCalculationException(
                    "Error in saving cardinality audit info for table: " + table + " error message: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void updateAuditInfo(Map<String, CardinalityCalculationAuditInfo> cardinalityAuditInfoMap) {
        log.debug("Updating cardinality info : {}", cardinalityAuditInfoMap);
        BulkRequest bulkRequestBuilder = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, CardinalityCalculationAuditInfo> mapEntry : cardinalityAuditInfoMap.entrySet()) {
            try {
                if (mapEntry.getValue() == null) {
                    throw new CardinalityCalculationException(
                            String.format("Illegal Update Audit Info Request - audit info is Null for Table - %s",
                                    mapEntry.getKey()));
                }
                bulkRequestBuilder.add(
                        new IndexRequest(CARDINALITY_AUDIT_INFO_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                                mapEntry.getKey()).source(JsonUtils.toBytes(mapEntry.getValue()), XContentType.JSON));
            } catch (Exception e) {
                log.error("Error while adding index request for updating audit info table: {}, audit info: {}",
                        mapEntry.getKey(), mapEntry.getValue(), e);
                throw new CardinalityMapStoreException("Error bulk saving meta: ", e);
            }
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error while bulk updating cardinality audit info with bulk request :{}", cardinalityAuditInfoMap,
                    e);
            throw new CardinalityCalculationException("Error while bulk updating cardinality audit info", e);
        }
    }

    @Override
    public CardinalityCalculationAuditInfoSummary fetchAuditSummary(boolean detailed) {
        CardinalityCalculationAuditInfoSummaryBuilder summaryBuilder = CardinalityCalculationAuditInfoSummary.builder();

        if (detailed) {
            summaryBuilder.auditInfo(fetchAuditInfo());
        }

        try {
            SearchResponse searchResponse = elasticsearchConnection.getClient()
                    .search(buildAuditSummaryRequest(), RequestOptions.DEFAULT);

            Map<String, Aggregation> aggregationMap = searchResponse.getAggregations()
                    .asMap();

            return summaryBuilder.count(getCountSummary(searchResponse, aggregationMap))
                    .timeTaken(getTimeTakenSummary(aggregationMap))
                    .build();


        } catch (IOException e) {
            log.error("Error while calculating audit summary: ", e);
            throw new CardinalityCalculationException("Error while calculating audit summary", e);
        }

    }

    private TimeTakenSummary getTimeTakenSummary(Map<String, Aggregation> aggregationMap) {
        Percentiles timeTakenPercentiles = (Percentiles) aggregationMap.get(TIME_TAKEN_PERCENTILE_AGGREGATION);
        TimeTakenSummaryBuilder timeTakenSummary = TimeTakenSummary.builder();

        timeTakenSummary.p50(timeTakenPercentiles.percentile(50.0));
        timeTakenSummary.p75(timeTakenPercentiles.percentile(75.0));
        timeTakenSummary.p95(timeTakenPercentiles.percentile(95.0));
        timeTakenSummary.p99(timeTakenPercentiles.percentile(99.0));
        timeTakenSummary.p999(timeTakenPercentiles.percentile(99.9));

        ParsedStats timeTakenStats = (ParsedStats) aggregationMap.get(TIME_TAKEN_STATS_AGGREGATION);
        timeTakenSummary.average(timeTakenStats.getAvg());
        return timeTakenSummary.build();
    }

    private CountSummary getCountSummary(SearchResponse searchResponse,
                                         Map<String, Aggregation> aggregationMap) {
        CountSummaryBuilder countSummaryBuilder = CountSummary.builder()
                .total(searchResponse.getHits()
                        .getTotalHits());
        Terms countAggregation = (Terms) aggregationMap.get(COUNT_AGGREGATION);
        Map<String, Long> statusCount = new HashMap<>();
        for (Terms.Bucket bucket : countAggregation.getBuckets()) {
            statusCount.put(bucket.getKey()
                    .toString(), bucket.getDocCount());
        }
        return countSummaryBuilder.status(statusCount)
                .build();
    }

    private SearchRequest buildAuditSummaryRequest() {
        SearchRequest searchRequest = new SearchRequest(CARDINALITY_AUDIT_INFO_INDEX).types(
                ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .indicesOptions(Utils.indicesOptions());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
                .query(QueryBuilders.existsQuery(STATUS_ATTRIBUTE))
                .aggregation(AggregationBuilders.terms(COUNT_AGGREGATION)
                        .field(STATUS_ATTRIBUTE))
                .aggregation(AggregationBuilders.percentiles(TIME_TAKEN_PERCENTILE_AGGREGATION)
                        .field(TIME_TAKEN_ATTRIBUTE)
                        .percentiles(50.0, 75.0, 95.0, 99.0, 99.9))
                .aggregation(AggregationBuilders.stats(TIME_TAKEN_STATS_AGGREGATION)
                        .field(TIME_TAKEN_ATTRIBUTE));
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    @Override
    public Map<String, CardinalityCalculationAuditInfo> fetchAuditInfo() {
        try {
            Map<String, CardinalityCalculationAuditInfo> auditInfoMap = new HashMap<>();

            SearchRequest searchRequest = new SearchRequest(CARDINALITY_AUDIT_INFO_INDEX);
            searchRequest.types(ElasticsearchUtils.DOCUMENT_TYPE_NAME);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(100);
            searchSourceBuilder.fetchSource(true);

            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(new TimeValue(60000));

            SearchResponse scrollResponse = elasticsearchConnection.getClient()
                    .search(searchRequest, RequestOptions.DEFAULT);

            do {
                for (SearchHit searchHit : scrollResponse.getHits()
                        .getHits()) {
                    auditInfoMap.put(searchHit.getId(),
                            JsonUtils.fromJson(searchHit.getSourceAsString(), CardinalityCalculationAuditInfo.class));
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollResponse.getScrollId());
                scrollRequest.scroll(TimeValue.timeValueSeconds(60000));
                scrollResponse = elasticsearchConnection.getClient()
                        .scroll(scrollRequest, RequestOptions.DEFAULT);
            } while (scrollResponse.getHits()
                    .getHits().length != 0);

            return auditInfoMap;
        } catch (Exception e) {
            log.error("Error in checking if any pending cardinality job is present error message: {}", e.getMessage(),
                    e);
            throw new CardinalityCalculationException(
                    "Error in checking if any pending cardinality job is present error message: " + e.getMessage(), e);
        }
    }

    private void estimateCardinality(final String table,
                                     final Collection<FieldMetadata> fields,
                                     long time,
                                     List<Exception> errors) {
        if (CollectionUtils.isNullOrEmpty(fields)) {
            log.info("No fields to estimate cardinality for table: {}", table);
            errors.add(new CardinalityCalculationException("No fields to estimate cardinality for table: " + table));
            return;
        }
        Map<String, FieldMetadata> fieldMap = fields.stream()
                .collect(Collectors.toMap(FieldMetadata::getField, fieldMetadata -> fieldMetadata, (lhs, rhs) -> lhs));

        final String index = ElasticsearchUtils.getCurrentIndex(ElasticsearchUtils.getValidName(table), time);
        final RestHighLevelClient client = elasticsearchConnection.getClient();
        Map<String, EstimationData> estimationData = estimateFirstPhaseData(table, index, client, fieldMap, errors);
        estimationData = estimateSecondPhaseData(table, index, client, estimationData, errors);
        estimationData.forEach((key, value) -> fieldMap.get(key)
                .setEstimationData(value));
    }

    private Map<String, EstimationData> estimateFirstPhaseData(String table,
                                                               String index,
                                                               RestHighLevelClient client,
                                                               Map<String, FieldMetadata> fields,
                                                               List<Exception> errors) {
        Map<String, EstimationData> estimationDataMap = Maps.newHashMap();

        log.debug("Estimating first phase data for table: {}", table);

        List<Map<String, FieldMetadata>> listOfMaps = fields.entrySet()
                .stream()
                .collect(mapSize(subListSize));

        for (Map<String, FieldMetadata> innerMap : listOfMaps) {
            try {
                MultiSearchRequest multiQuery = new MultiSearchRequest();
                innerMap.values()
                        .forEach(fieldMetadata -> {
                            String field = fieldMetadata.getField();
                            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
                                    .query(QueryBuilders.existsQuery(field));
                            switch (fieldMetadata.getType()) {
                                case STRING:
                                    evaluateStringAggregation(table, field, fieldMetadata.getType(),
                                            searchSourceBuilder);
                                    break;
                                case INTEGER:
                                case LONG:
                                case FLOAT:
                                case DOUBLE:
                                    evaluateDoubleAggregation(table, field, fieldMetadata.getType(),
                                            searchSourceBuilder);
                                    break;
                                default:
                            }
                            SearchRequest query = new SearchRequest(index).indicesOptions(Utils.indicesOptions())
                                    .source(searchSourceBuilder);
                            multiQuery.add(query);
                        });
                Stopwatch stopwatch = Stopwatch.createStarted();
                MultiSearchResponse multiResponse;
                try {
                    multiResponse = executeFirstPhaseMultiSearchQuery(multiQuery, client, table);
                } finally {
                    log.info("Ran cardinality query on table {} for {} fields took {} ms", table, fields.size(),
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }
                if (null != multiResponse) {
                    handleFirstPhaseMultiSearchResponse(multiResponse, table, fields, estimationDataMap, errors);
                }
            } catch (Exception e) {
                errors.add(e);
            }
        }
        return estimationDataMap;
    }

    private MultiSearchResponse executeFirstPhaseMultiSearchQuery(MultiSearchRequest multiQuery,
                                                                  RestHighLevelClient client,
                                                                  String table) {
        try {
            return client.msearch(multiQuery, RequestOptions.DEFAULT);
        } catch (IOException e) {
            List<String> multiQueryList = multiQuery.requests()
                    .stream()
                    .map(searchRequest -> searchRequest.source()
                            .toString())
                    .collect(Collectors.toList());
            log.error("Error while executing cardinality estimation query for table:{}, query:{}", table,
                    multiQueryList, e);
            throw new CardinalityCalculationException(
                    "Error while executing cardinality estimation query for table: " + table + " , query: "
                            + multiQueryList, e);
        }
    }

    private void handleFirstPhaseMultiSearchResponse(MultiSearchResponse multiResponse,
                                                     String table,
                                                     Map<String, FieldMetadata> fields,
                                                     Map<String, EstimationData> estimationDataMap,
                                                     List<Exception> errors) {
        for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
            SearchResponse response = validateAndGetSearchResponse(item, table);
            if (response == null) {
                continue;
            }
            final long hits = response.getHits()
                    .getTotalHits();
            Map<String, Aggregation> output = response.getAggregations()
                    .asMap();
            output.forEach((key, value) -> {
                try {
                    // for every key in output aggregation response , get the fieldMetaData we have
                    FieldMetadata fieldMetadata = fields.get(key);

                    // for double, long, integer, float field the aggregation key is - "_"+fieldName. so replace with "" and try
                    if (fieldMetadata == null) {
                        fieldMetadata = fields.get(key.replace("_", ""));
                    }
                    if (fieldMetadata == null) {
                        log.error("Field meta data not found for table: {} for field: {} "
                                        + "while handling first phase multi search response for cardinality estimation", table,
                                key);
                        throw new CardinalityCalculationException(
                                "Field meta data not found for table " + table + " field: " + key
                                        + " while handling first phase multi search response for cardinality estimation");
                    }
                    switch (fieldMetadata.getType()) {
                        case STRING:
                            evaluateStringEstimation(value, table, key, fieldMetadata.getType(), estimationDataMap,
                                    hits);
                            break;
                        case INTEGER:
                        case LONG:
                        case FLOAT:
                        case DOUBLE:
                            evaluateDoubleEstimation(value, table, key, fieldMetadata.getType(), estimationDataMap,
                                    hits);
                            break;
                        case BOOLEAN:
                            evaluateBooleanEstimation(key, estimationDataMap);
                            break;
                        default:
                    }
                } catch (Exception e) {
                    log.error("Error while handling first phase multi search response for key: {}, value: {}, error: ",
                            key, value, e);
                    errors.add(e);
                }

            });
        }
    }

    private void evaluateStringAggregation(String table,
                                           String field,
                                           FieldType type,
                                           SearchSourceBuilder searchSourceBuilder) {
        log.info("Adding aggregationType:{} to multiquery for estimation, table:{} field:{} type:{} ", CARDINALITY,
                table, field, type);
        searchSourceBuilder.aggregation(AggregationBuilders.cardinality(field)
                .field(field)
                .precisionThreshold(PRECISION_THRESHOLD));
    }

    private void evaluateDoubleAggregation(String table,
                                           String field,
                                           FieldType type,
                                           SearchSourceBuilder searchSourceBuilder) {
        log.info("Adding aggregationType:{} to multiquery for estimation, table:{} field:{} type:{} ", "percentile",
                table, field, type);
        searchSourceBuilder.aggregation(AggregationBuilders.percentiles(field)
                .field(field)
                .percentiles(10, 20, 30, 40, 50, 60, 70, 80, 90, 100));
        searchSourceBuilder.aggregation(AggregationBuilders.cardinality("_" + field)
                .field(field)
                .precisionThreshold(PRECISION_THRESHOLD));
    }

    private void evaluateStringEstimation(Aggregation value,
                                          String table,
                                          String key,
                                          FieldType type,
                                          Map<String, EstimationData> estimationDataMap,
                                          long hits) {
        Cardinality cardinality = (Cardinality) value;
        log.info(
                "Populating string cardinality estimation data for table:{} field:{} type:{} aggregationType:{} value:{} ",
                table, key, type, CARDINALITY, cardinality.getValue());
        estimationDataMap.put(key, CardinalityEstimationData.builder()
                .cardinality(cardinality.getValue())
                .count(hits)
                .build());
    }

    private void evaluateDoubleEstimation(Aggregation value,
                                          String table,
                                          String key,
                                          FieldType type,
                                          Map<String, EstimationData> estimationDataMap,
                                          long hits) {
        if (value instanceof Percentiles) {
            Percentiles percentiles = (Percentiles) value;
            double[] values = new double[10];
            for (int i = 10; i <= 100; i += 10) {
                final Double percentile = percentiles.percentile(i);
                values[(i / 10) - 1] = percentile.isNaN()
                        ? 0
                        : percentile;
            }
            log.info(
                    "Populating double cardinality estimation data for table:{} field:{} type:{} aggregationType:{} value:{}",
                    table, key, type, "percentile", values);
            estimationDataMap.put(key, PercentileEstimationData.builder()
                    .values(values)
                    .count(hits)
                    .build());
        } else if (value instanceof Cardinality) {
            Cardinality cardinality = (Cardinality) value;
            log.info(
                    "Populating double cardinality estimation data for table:{} field:{} type:{} aggregationType:{} value:{}",
                    table, key, type, CARDINALITY, cardinality.getValue());
            EstimationData estimationData = estimationDataMap.get(key.replace("_", ""));
            if (estimationData instanceof PercentileEstimationData) {
                ((PercentileEstimationData) estimationData).setCardinality(cardinality.getValue());
            } else {
                estimationDataMap.put(key.replace("_", ""), PercentileEstimationData.builder()
                        .cardinality(cardinality.getValue())
                        .build());
            }
        }
    }

    private void evaluateBooleanEstimation(String key,
                                           Map<String, EstimationData> estimationDataMap) {
        estimationDataMap.put(key, FixedEstimationData.builder()
                .count(2)
                .build());
    }

    private Map<String, EstimationData> estimateSecondPhaseData(String table,
                                                                String index,
                                                                RestHighLevelClient client,
                                                                Map<String, EstimationData> estimationData,
                                                                List<Exception> errors) {
        long maxDocuments = estimationData.values()
                .stream()
                .map(EstimationData::getCount)
                .max(Comparator.naturalOrder())
                .orElse(0L);
        if (maxDocuments == 0) {
            return estimationData;
        }
        log.debug("Estimating second phase data for table: {}", table);
        Map<String, EstimationData> estimationDataMap = Maps.newHashMap(estimationData);

        List<Map<String, EstimationData>> listOfMaps = estimationData.entrySet()
                .stream()
                .collect(mapSize(subListSize));

        for (Map<String, EstimationData> dataMap : listOfMaps) {
            try {
                MultiSearchRequest multiQuery = new MultiSearchRequest();
                // keep a flag to track if search request was added to multiquery
                AtomicBoolean searchRequestAdded = new AtomicBoolean(false);
                dataMap.forEach((key, value) -> value.accept(new EstimationDataVisitor<Void>() {
                    @Override
                    public Void visit(FixedEstimationData fixedEstimationData) {
                        return null;
                    }

                    @Override
                    public Void visit(PercentileEstimationData percentileEstimationData) {
                        return null;
                    }

                    @Override
                    public Void visit(CardinalityEstimationData cardinalityEstimationData) {
                        if (cardinalityEstimationData.getCount() > 0
                                && cardinalityEstimationData.getCardinality() > 0) {
                            int countToCardinalityRatio = (int) (cardinalityEstimationData.getCount()
                                    / cardinalityEstimationData.getCardinality());
                            int documentToCountRatio = (int) (maxDocuments / cardinalityEstimationData.getCount());
                            log.info(
                                    "Calculated counts for second phase data for estimation, field: {}, count: {}, cardinality: {} "
                                            + ",countToCardinalityRatio: {}, maxDocuments: {}, documentToCountRatio: {}",
                                    key, cardinalityEstimationData.getCount(),
                                    cardinalityEstimationData.getCardinality(), countToCardinalityRatio, maxDocuments,
                                    documentToCountRatio);
                            if (cardinalityEstimationData.getCardinality() <= 100 || (countToCardinalityRatio > 100
                                    && documentToCountRatio < 100
                                    && cardinalityEstimationData.getCardinality() <= 5000)) {
                                log.info(
                                        "Evaluating second phase data for estimation, field: {}, count: {}, cardinality: {}"
                                                + " ,countToCardinalityRatio: {}, maxDocuments: {}, documentToCountRatio: {}",
                                        key, cardinalityEstimationData.getCount(),
                                        cardinalityEstimationData.getCardinality(), countToCardinalityRatio,
                                        maxDocuments, documentToCountRatio);
                                multiQuery.add(new SearchRequest(index).indicesOptions(Utils.indicesOptions())
                                        .source(new SearchSourceBuilder().query(QueryBuilders.existsQuery(key))
                                                .aggregation(AggregationBuilders.terms(key)
                                                        .field(key)
                                                        .size(ElasticsearchQueryUtils.QUERY_SIZE))
                                                .size(0)));
                                searchRequestAdded.set(true);
                            } else {
                                log.info("Skipped evaluating phase data for estimation, condition did not meet, "
                                                + "field: {}, count: {}, cardinality: {}"
                                                + " ,countToCardinalityRatio: {}, maxDocuments: {}, documentToCountRatio: {}",
                                        key, cardinalityEstimationData.getCount(),
                                        cardinalityEstimationData.getCardinality(), countToCardinalityRatio,
                                        maxDocuments, documentToCountRatio);
                            }
                        }
                        return null;
                    }

                    @Override
                    public Void visit(TermHistogramEstimationData termHistogramEstimationData) {
                        return null;
                    }
                }));

                MultiSearchResponse multiResponse = null;

                if (searchRequestAdded.get()) {
                    multiResponse = getMultiResponse(table, client, multiQuery);
                } else {
                    log.info("Skipped evaluating phase data for estimation, no search request added for fields");
                }

                if (null != multiResponse) {
                    handleSecondPhaseMultiSearchResponse(multiResponse, table, estimationDataMap);
                }
            } catch (Exception e) {
                errors.add(e);
            }

        }

        return estimationDataMap;
    }

    private MultiSearchResponse getMultiResponse(String table,
                                                 RestHighLevelClient client,
                                                 MultiSearchRequest multiQuery) {
        MultiSearchResponse multiResponse;
        try {
            multiResponse = client.msearch(multiQuery, RequestOptions.DEFAULT);
        } catch (IOException e) {
            List<String> multiQueryList = multiQuery.requests()
                    .stream()
                    .map(searchRequest -> searchRequest.source()
                            .toString())
                    .collect(Collectors.toList());
            log.error("Error occurred while running query for second phase estimation data, query: {}", multiQueryList,
                    e);
            throw new CardinalityCalculationException("Error occurred while running query for table: " + table
                    + " for second phase estimation data, query: " + multiQueryList);
        }
        return multiResponse;
    }

    private void handleSecondPhaseMultiSearchResponse(MultiSearchResponse multiResponse,
                                                      String table,
                                                      Map<String, EstimationData> estimationDataMap) {
        for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
            SearchResponse response = validateAndGetSearchResponse(item, table);
            if (response == null) {
                continue;
            }

            final long hits = response.getHits()
                    .getTotalHits();
            Map<String, Aggregation> output = response.getAggregations()
                    .asMap();
            output.forEach((key, value) -> {
                Terms terms = (Terms) output.get(key);
                estimationDataMap.put(key, TermHistogramEstimationData.builder()
                        .count(hits)
                        .termCounts(terms.getBuckets()
                                .stream()
                                .collect(Collectors.toMap(Terms.Bucket::getKeyAsString, Terms.Bucket::getDocCount)))
                        .build());
            });
        }
    }

    private SearchResponse validateAndGetSearchResponse(MultiSearchResponse.Item item,
                                                        String table) {
        if (item.isFailure()) {
            log.error(
                    "Error in multisearch response for cardinality, table:{} , item: {}, search response :{}, failureMessage:{}, error: ",
                    table, item, item.getResponse(), item.getFailureMessage(), item.getFailure());
            throw new CardinalityCalculationException(
                    "Error in multisearch response for cardinality, table:" + table + " , item: " + item
                            + " , search response: " + item.getResponse() + ", failureMessage:"
                            + item.getFailureMessage());
        }
        SearchResponse response = item.getResponse();
        if (null == response.getAggregations()) {
            log.info("No aggregation response for cardinality query, item: {}, search response :{}, table:{}", item,
                    item.getResponse(), table);
            return null;
        }

        return response;
    }

}
