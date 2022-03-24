package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.GeoAggregationRequest;
import com.flipkart.foxtrot.common.GeoAggregationResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.visitor.CountPrecisionThresholdVisitorAdapter;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.core.querystore.actions.Utils.statsString;
import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

@AnalyticsProvider(opcode = "geo_aggregation", request = GeoAggregationRequest.class, response = GeoAggregationResponse.class, cacheable = true)
@SuppressWarnings("squid:CallToDeprecatedMethod")
@Slf4j
public class GeoAggregationAction extends Action<GeoAggregationRequest> {

    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    public GeoAggregationAction(GeoAggregationRequest parameter,
                                AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.elasticsearchTuningConfig = analyticsLoader.getElasticsearchTuningConfig();
    }

    @Override
    public void preprocess() {
        // Nothing to Preprocess
    }

    @Override
    public String getMetricKey() {
        return null;
    }

    @Override
    public String getRequestCacheKey() {
        long filterHashKey = 0L;
        GeoAggregationRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.accept(getCacheKeyVisitor());
            }
        }
        if (null != query.getUniqueCountOn()) {
            filterHashKey += 31 * query.getUniqueCountOn()
                    .hashCode();
        }

        if (null != query.getAggregationField()) {
            filterHashKey += 31 * query.getAggregationField()
                    .hashCode();
        }

        if (Objects.nonNull(query.getAggregationType())) {
            filterHashKey += 31 * query.getAggregationType()
                    .hashCode();
        }
        filterHashKey += 31 * query.getLocationField()
                .hashCode();
        filterHashKey += query.getGridLevel();

        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public SearchRequest getRequestBuilder(GeoAggregationRequest parameter,
                                           List<Filter> extraFilters) {
        return new SearchRequest(ElasticsearchUtils.getIndices(parameter.getTable(), parameter)).indicesOptions(
                Utils.indicesOptions())
                .source(new SearchSourceBuilder().size(QUERY_SIZE)
                        .timeout(new TimeValue(getGetQueryTimeout(), TimeUnit.MILLISECONDS))
                        .query(ElasticsearchQueryUtils.translateFilter(parameter, extraFilters))
                        .aggregation(buildAggregation()));
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response,
                                      GeoAggregationRequest parameter) {
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        if (aggregations != null) {
            return buildResponse(parameter, aggregations);
        } else {
            return new GeoAggregationResponse(Collections.<String, Object>emptyMap());
        }
    }

    @Override
    public void validateImpl(GeoAggregationRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (Strings.isNullOrEmpty(parameter.getLocationField())) {
            validationErrors.add("Location Field is required to do Geo Aggregation");
        }

        if (parameter.getUniqueCountOn() != null && parameter.getUniqueCountOn()
                .isEmpty()) {
            validationErrors.add("uniqueCountOn cannot be empty (can be null)");
        }

        if (parameter.getAggregationField() != null && parameter.getAggregationField()
                .isEmpty()) {
            validationErrors.add("aggregation field cannot be empty (can be null)");
        }

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    private GeoAggregationResponse buildResponse(GeoAggregationRequest request,
                                                 Aggregations aggregations) {
        return new GeoAggregationResponse(getMap(request, aggregations));
    }

    @Override
    public ActionResponse execute(GeoAggregationRequest parameter) {
        SearchRequest query = getRequestBuilder(parameter, Collections.emptyList());
        try {
            SearchResponse response = getConnection().getClient()
                    .search(query);
            return getResponse(response, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    private AbstractAggregationBuilder buildAggregation() {
        val parameter = getParameter();
        val aggregation = AggregationBuilders.geohashGrid(
                Utils.sanitizeFieldForAggregation(parameter.getLocationField()))
                .field(parameter.getLocationField())
                .precision(getParameter().getGridLevel());
        for (val subAgg : buildSubAggregation(getParameter())) {
            aggregation.subAggregation(subAgg);
        }
        return aggregation;
    }

    private Set<AggregationBuilder> buildSubAggregation(GeoAggregationRequest geoAggregationRequest) {
        // Keep this for backward compatibility to support uniqueCountOn attribute coming from old requests
        if (!Strings.isNullOrEmpty(geoAggregationRequest.getUniqueCountOn())) {
            return Sets.newHashSet(buildCardinalityAggregation(geoAggregationRequest.getUniqueCountOn(), geoAggregationRequest));
        }

        if (Strings.isNullOrEmpty(geoAggregationRequest.getAggregationField())) {
            return Sets.newHashSet();
        }

        boolean isNumericField = Utils.isNumericField(getTableMetadataManager(), geoAggregationRequest.getTable(),
                geoAggregationRequest.getAggregationField());
        final AbstractAggregationBuilder groupAggStats;
        if (isNumericField) {
            groupAggStats = Utils.buildStatsAggregation(geoAggregationRequest.getAggregationField(),
                    Collections.singleton(geoAggregationRequest.getAggregationType()));
        } else {
            groupAggStats = buildCardinalityAggregation(geoAggregationRequest.getAggregationField(), geoAggregationRequest);
        }
        return Sets.newHashSet(groupAggStats);
    }

    private CardinalityAggregationBuilder buildCardinalityAggregation(String aggregationField,
                                                                      GeoAggregationRequest groupRequest) {
        return Utils.buildCardinalityAggregation(aggregationField, groupRequest.accept(
                new CountPrecisionThresholdVisitorAdapter(elasticsearchTuningConfig.getPrecisionThreshold())));
    }

    private Map<String, Object> getMap(GeoAggregationRequest request,
                                       Aggregations aggregations) {

        ParsedGeoHashGrid terms = aggregations.get(Utils.sanitizeFieldForAggregation(request.getLocationField()));
        Map<String, Object> levelCount = Maps.newHashMap();
        for (org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid.Bucket bucket : terms.getBuckets()) {
            if (!Strings.isNullOrEmpty(getParameter().getUniqueCountOn())) {
                String key = Utils.sanitizeFieldForAggregation(getParameter().getUniqueCountOn());
                Cardinality cardinality = bucket.getAggregations()
                        .get(key);
                levelCount.put(String.valueOf(bucket.getKey()), cardinality.getValue());
            } else if (!Strings.isNullOrEmpty(getParameter().getAggregationField())) {
                boolean isNumericField = Utils.isNumericField(getTableMetadataManager(), request.getTable(),
                        request.getAggregationField());

                if (isNumericField) {
                    String metricKey = Utils.getExtendedStatsAggregationKey(getParameter().getAggregationField());
                    levelCount.put(String.valueOf(bucket.getKey()), Utils.toStats(bucket.getAggregations()
                            .get(metricKey))
                            .get(statsString(getParameter().getAggregationType())));
                } else {
                    String metricKey = Utils.sanitizeFieldForAggregation(getParameter().getAggregationField());
                    Cardinality cardinality = bucket.getAggregations()
                            .get(metricKey);
                    levelCount.put(String.valueOf(bucket.getKey()), cardinality.getValue());
                }
            } else {
                levelCount.put(String.valueOf(bucket.getKey()), bucket.getDocCount());
            }

        }
        return levelCount;

    }
}
