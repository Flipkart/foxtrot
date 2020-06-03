package com.flipkart.foxtrot.core.funnel.persistence;

import static com.collections.CollectionUtils.nullAndEmptySafeValueList;
import static com.collections.CollectionUtils.nullSafeMap;
import static com.flipkart.foxtrot.common.exception.ErrorCode.EXECUTION_EXCEPTION;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.DELETED;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.EVENT_ATTRIBUTES;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.FIELD_VS_VALUES;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.FUNNEL_STATUS;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.ID;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.DOT;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.TYPE;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.funnel.model.EventAttributes;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import com.flipkart.foxtrot.core.funnel.services.MappingService;
import com.flipkart.foxtrot.core.funnel.services.PreProcessFilter;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.inject.Inject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 25/09/18
 ***/
public class ElasticsearchFunnelStore implements FunnelStore {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFunnelStore.class);


    protected final ElasticsearchConnection connection;
    protected final MappingService mappingService;
    protected final FunnelConfiguration funnelConfiguration;


    @Inject
    public ElasticsearchFunnelStore(ElasticsearchConnection connection,
                                    MappingService mappingService,
                                    FunnelConfiguration funnelConfiguration) {
        this.connection = connection;
        this.mappingService = mappingService;
        this.funnelConfiguration = funnelConfiguration;
    }

    @Override
    public void save(Funnel funnel) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(funnelConfiguration.getFunnelIndex())
                    .setType(TYPE)
                    .setId(funnel.getDocumentId())
                    .setSource(JsonUtils.toBytes(funnel), XContentType.JSON)
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setOpType(OpType.CREATE)
                    .execute()
                    .get();
        } catch (Exception e) {
            logger.error(String.format("error saving funnel with name : %s", funnel.getName()));
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel save failed", e);
        }
    }

    @Override
    public Funnel getByDocumentId(final String documentId) throws FoxtrotException {
        try {
            GetResponse response = connection.getClient()
                    .prepareGet()
                    .setIndex(funnelConfiguration.getFunnelIndex())
                    .setType(TYPE)
                    .setId(documentId)
                    .get();
            if (!response.isExists() || response.isSourceEmpty()) {
                return null;
            }
            return JsonUtils.fromJson(response.getSourceAsString(), Funnel.class);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel get by document id failed", e);
        }
    }

    @Override
    public Funnel getByFunnelId(String funnelId) throws FoxtrotException {
        QueryBuilder query = new TermQueryBuilder(FunnelAttributes.ID, funnelId);
        try {
            SearchHits response = connection.getClient()
                    .prepareSearch(funnelConfiguration.getFunnelIndex())
                    .setTypes(TYPE)
                    .setSize(1)
                    .setQuery(query)
                    .execute()
                    .actionGet()
                    .getHits();
            if (response == null || response.getTotalHits() == 0) {
                return null;
            }
            return JsonUtils.fromJson(response.getAt(0)
                    .getSourceAsString(), Funnel.class);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel get by funnel id failed", e);
        }

    }

    @Override
    public List<Funnel> searchSimilar(Funnel funnel) throws FunnelException {
        List<Funnel> similarFunnels = new ArrayList<>();
        BoolQueryBuilder esRequest = buildSimilarFunnelSearchQuery(funnel);
        SearchHits searchHits;
        try {
            SearchRequestBuilder requestBuilder = connection.getClient()
                    .prepareSearch(funnelConfiguration.getFunnelIndex())
                    .setTypes(TYPE)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(esRequest)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setSize(funnelConfiguration.getQuerySize());
            SearchResponse response = requestBuilder.execute()
                    .actionGet();
            searchHits = response.getHits();
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Error fetching similar funnels", e);
        }
        if (searchHits.totalHits == 0) {
            return similarFunnels;
        }

        for (SearchHit searchHit : nullAndEmptySafeValueList(searchHits.getHits())) {
            Funnel existingFunnel = JsonUtils.fromJson(searchHit.getSourceAsString(), Funnel.class);
            similarFunnels.add(existingFunnel);
        }
        return similarFunnels;
    }

    @Override
    public void update(Funnel funnel) throws FoxtrotException {
        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(funnelConfiguration.getFunnelIndex())
                    .type(TYPE)
                    .id(funnel.getDocumentId())
                    .doc(JsonUtils.toBytes(funnel), XContentType.JSON)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            connection.getClient()
                    .update(updateRequest)
                    .get();
            logger.info("Updated Funnel: {}", funnel);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION,
                    "Funnel couldn't be updated for Id: " + funnel.getId() + " Error Message: " + e.getMessage(), e);
        }
    }


    @Override
    public List<Funnel> getAll(boolean deleted) throws FoxtrotException {

        int maxSize = 1000;
        List<Funnel> funnels = new ArrayList<>();
        try {
            SearchRequestBuilder searchRequestBuilder = connection.getClient()
                    .prepareSearch(funnelConfiguration.getFunnelIndex())
                    .setTypes(TYPE)
                    .setSize(maxSize);

            if (!deleted) {
                QueryBuilder query = new TermQueryBuilder(FunnelAttributes.DELETED, deleted);
                searchRequestBuilder.setQuery(query);
            }

            SearchResponse response = searchRequestBuilder.execute()
                    .actionGet();
            for (SearchHit hit : CollectionUtils.nullAndEmptySafeValueList(response.getHits()
                    .getHits())) {
                funnels.add(JsonUtils.fromJson(hit.getSourceAsString(), Funnel.class));
            }
            return funnels;
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel get all failed", e);
        }
    }

    @Override
    public FunnelFilterResponse search(FilterRequest filterRequest) throws FoxtrotException {
        preProcessFilterRequest(filterRequest);
        List<Funnel> funnels = new ArrayList<>();
        SearchHits searchHits;
        try {
            searchHits = connection.getClient()
                    .prepareSearch(funnelConfiguration.getFunnelIndex())
                    .setTypes(TYPE)
                    .setQuery(new ElasticSearchQueryGenerator().genFilter(filterRequest.getFilters()))
                    .addSort(SortBuilders.fieldSort(filterRequest.getFieldName())
                            .order(filterRequest.getSortOrder()))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setFrom(filterRequest.getFrom())
                    .setSize(filterRequest.getSize())
                    .execute()
                    .actionGet()
                    .getHits();
            long hitsCount = searchHits.getTotalHits();
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                funnels.add(JsonUtils.fromJson(searchHit.getSourceAsString(), Funnel.class));
            }
            return new FunnelFilterResponse(hitsCount, funnels);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel search failed", e);
        }
    }

    @Override
    public void delete(final String documentId) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(funnelConfiguration.getFunnelIndex())
                    .setType(TYPE)
                    .setId(documentId)
                    .execute()
                    .actionGet();
            logger.info("Deleted Funnel with document id: {}", documentId);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel deletion_failed", e);
        }
    }

    @Override
    public Funnel getLatestFunnel() throws FoxtrotException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        TermQueryBuilder statusQueryBuilder = new TermQueryBuilder(FUNNEL_STATUS, FunnelStatus.APPROVED.name());
        TermQueryBuilder deletedQueryBuilder = new TermQueryBuilder(DELETED, false);
        boolQueryBuilder.must(statusQueryBuilder);
        boolQueryBuilder.must(deletedQueryBuilder);
        try {
            SearchResponse response = connection.getClient()
                    .prepareSearch(funnelConfiguration.getFunnelIndex())
                    .setTypes(TYPE)
                    .setQuery(boolQueryBuilder)
                    .addSort(SortBuilders.fieldSort(ID)
                            .order(SortOrder.DESC))
                    .get();
            if (response.getHits()
                    .getTotalHits() == 0) {
                return null;
            }
            return JsonUtils.fromJson(response.getHits()
                    .getAt(0)
                    .getSourceAsString(), Funnel.class);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Failed to get latest funnel", e);
        }
    }

    @Override
    public FunnelDropdownConfig getFunnelDropdownValues() {
        return funnelConfiguration.getFunnelDropdownConfig();
    }

    private BoolQueryBuilder buildSimilarFunnelSearchQuery(Funnel funnel) throws FoxtrotException {
        BoolQueryBuilder outerQueryBuilder = QueryBuilders.boolQuery();

        //Field VS Value Query
        BoolQueryBuilder fieldVsValuesQueryBuilder = buildFieldVsValuesQuery(funnel.getFieldVsValues());

        //Event Attributes query
        BoolQueryBuilder eventAttributesQueryBuilder = buildEventAttributesQuery(funnel);

        outerQueryBuilder.must(QueryBuilders.nestedQuery(FIELD_VS_VALUES, fieldVsValuesQueryBuilder, ScoreMode.Avg));
        outerQueryBuilder.must(QueryBuilders.nestedQuery(EVENT_ATTRIBUTES, eventAttributesQueryBuilder, ScoreMode.Avg));
        return outerQueryBuilder;
    }

    private BoolQueryBuilder buildFieldVsValuesQuery(Map<String, List<String>> fieldVsValues) {
        BoolQueryBuilder boolFieldVsValuesQueryBuilder = QueryBuilders.boolQuery();

        for (Map.Entry<String, List<String>> entry : nullSafeMap(fieldVsValues).entrySet()) {
            String field = FIELD_VS_VALUES + DOT + entry.getKey();
            for (String value : entry.getValue()) {
                BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
                mustQueryBuilder.must(new TermQueryBuilder(field, value));
                boolFieldVsValuesQueryBuilder.must(mustQueryBuilder);
            }
        }
        return boolFieldVsValuesQueryBuilder;
    }

    private BoolQueryBuilder buildEventAttributesQuery(Funnel funnel) {
        BoolQueryBuilder boolEventAttributesQueryBuilder = QueryBuilders.boolQuery();

        try {
            Map<String, List<Object>> eventAttributesMap = new HashMap<>();
            for (EventAttributes eventAttributes : nullAndEmptySafeValueList(funnel.getEventAttributes())) {
                Field[] eventAttributesFields = EventAttributes.class.getDeclaredFields();
                for (Field eventAttributeField : nullAndEmptySafeValueList(eventAttributesFields)) {
                    if (eventAttributeField.isSynthetic()) {
                        continue;
                    }
                    eventAttributeField.setAccessible(true);
                    if (eventAttributeField.get(eventAttributes) != null) {
                        String key = EVENT_ATTRIBUTES + DOT + eventAttributeField.getName();
                        if (!eventAttributesMap.containsKey(key)) {
                            eventAttributesMap.put(key, new ArrayList<>(
                                    Collections.singletonList(eventAttributeField.get(eventAttributes))));
                        } else {
                            eventAttributesMap.get(key)
                                    .add(eventAttributeField.get(eventAttributes));
                        }
                    }
                }
            }

            for (Map.Entry<String, List<Object>> entry : nullSafeMap(eventAttributesMap).entrySet()) {
                BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
                mustQueryBuilder.must(new TermsQueryBuilder(entry.getKey(), entry.getValue()));
                boolEventAttributesQueryBuilder.must(mustQueryBuilder);
            }
        } catch (IllegalAccessException e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Error in creating Event Attributes Query.", e);
        }

        return boolEventAttributesQueryBuilder;
    }

    private void preProcessFilterRequest(FilterRequest filterRequest) throws FoxtrotException {
        PreProcessFilter preProcessFilter = new PreProcessFilter();
        try {
            preProcessFilter.preProcess(filterRequest, mappingService, funnelConfiguration.getFunnelIndex());
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION,
                    String.format("Error in preProcessing filterRequest: %s", e.getMessage()), e);
        }
    }

}
