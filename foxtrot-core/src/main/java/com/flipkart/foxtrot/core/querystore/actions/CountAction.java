package com.flipkart.foxtrot.core.querystore.actions;

import static com.flipkart.foxtrot.core.util.OpensearchQueryUtils.QUERY_SIZE;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.visitor.CountPrecisionThresholdVisitorAdapter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.config.OpensearchTuningConfig;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
import com.flipkart.foxtrot.core.util.OpensearchQueryUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Created by rishabh.goyal on 02/11/14.
 */

@AnalyticsProvider(opcode = "count", request = CountRequest.class, response = CountResponse.class, cacheable = true)
public class CountAction extends Action<CountRequest> {

    private final OpensearchTuningConfig opensearchTuningConfig;

    public CountAction(CountRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.opensearchTuningConfig = analyticsLoader.getOpensearchTuningConfig();
    }


    @Override
    public void preprocess() {
        getParameter().setTable(OpensearchUtils.getValidTableName(getParameter().getTable()));
        // Null field implies complete doc count
        if (getParameter().getField() != null) {
            Filter existsFilter = new ExistsFilter(getParameter().getField());
            if (!getParameter().getFilters().contains(existsFilter)) {
                getParameter().getFilters()
                        .add(new ExistsFilter(getParameter().getField()));
            }
        }
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public String getRequestCacheKey() {
        preprocess();
        long filterHashKey = 0L;
        CountRequest request = getParameter();
        if (null != request.getFilters()) {
            for (Filter filter : request.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }

        filterHashKey += 31 * (request.isDistinct()
                ? "TRUE".hashCode()
                : "FALSE".hashCode());
        filterHashKey += 31 * (request.getField() != null
                ? request.getField()
                .hashCode()
                : "COLUMN".hashCode());
        return String.format("count-%s-%d", request.getTable(), filterHashKey);
    }

    @Override
    public void validateImpl(CountRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (parameter.isDistinct() && CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(CountRequest parameter) {
        SearchRequest request = getRequestBuilder(parameter, Collections.emptyList());

        try {
            SearchResponse response = getConnection()
                    .getClient()
                    .search(request, RequestOptions.DEFAULT);
            return getResponse(response, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);

        }
    }

    @Override
    public SearchRequest getRequestBuilder(CountRequest parameter, List<Filter> extraFilters) {
        if (parameter.isDistinct()) {
            try {
                return new SearchRequest(OpensearchUtils.getIndices(parameter.getTable(), parameter))
                        .indicesOptions(Utils.indicesOptions())
                        .source(new SearchSourceBuilder()
                                .size(QUERY_SIZE)
                                .query(OpensearchQueryUtils.translateFilter(parameter, extraFilters))
                                .aggregation(Utils.buildCardinalityAggregation(parameter.getField(),
                                        parameter.accept(new CountPrecisionThresholdVisitorAdapter(
                                                opensearchTuningConfig.getPrecisionThreshold())))))
                        ;
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(parameter, e);
            }
        } else {
            try {
                return new SearchRequest(OpensearchUtils.getIndices(parameter.getTable(), parameter))
                        .indicesOptions(Utils.indicesOptions())
                        .source(new SearchSourceBuilder()
                                .size(QUERY_SIZE)
                                .query(OpensearchQueryUtils.translateFilter(parameter, extraFilters)));
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(parameter, e);
            }
        }
    }

    @Override
    public ActionResponse getResponse(org.opensearch.action.ActionResponse response, CountRequest parameter) {
        if (parameter.isDistinct()) {
            Aggregations aggregations = ((SearchResponse) response).getAggregations();
            Cardinality cardinality = aggregations.get(Utils.sanitizeFieldForAggregation(parameter.getField()));
            if (cardinality == null) {
                return new CountResponse(0);
            } else {
                return new CountResponse(cardinality.getValue());
            }
        } else {
            return new CountResponse(((SearchResponse) response).getHits().getHits().length);
        }

    }
}
