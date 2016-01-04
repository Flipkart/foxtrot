/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.*;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 1:00 PM
 */
@AnalyticsProvider(opcode = "query", request = Query.class, response = QueryResponse.class, cacheable = false)
public class FilterAction extends Action<Query> {

    public FilterAction(Query parameter,
                        TableMetadataManager tableMetadataManager,
                        DataStore dataStore,
                        QueryStore queryStore,
                        ElasticsearchConnection connection,
                        String cacheToken,
                        CacheManager cacheManager) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        Query query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        filterHashKey += 31 * (query.getSort() != null ? query.getSort().hashCode() : "SORT".hashCode());

        return String.format("%s-%d-%d-%d", query.getTable(),
                query.getFrom(), query.getLimit(), filterHashKey);
    }

    @Override
    public QueryResponse execute(Query parameter) throws FoxtrotException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters() || parameter.getFilters().isEmpty()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        if (null == parameter.getSort()) {
            ResultSort resultSort = new ResultSort();
            resultSort.setField("_timestamp");
            resultSort.setOrder(ResultSort.Order.desc);
            parameter.setSort(resultSort);
        }
        SearchRequestBuilder search;
        try {
            search = getConnection().getClient().prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setFrom(parameter.getFrom())
                    .addSort(parameter.getSort().getField(),
                            ResultSort.Order.desc == parameter.getSort().getOrder() ? SortOrder.DESC : SortOrder.ASC)
                    .setSize(parameter.getLimit());
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        try {
            SearchResponse response = search.execute().actionGet();
            List<String> ids = new ArrayList<>();
            SearchHits searchHits = response.getHits();
            for (SearchHit searchHit : searchHits) {
                ids.add(searchHit.getId());
            }
            if (ids.isEmpty()) {
                return new QueryResponse(Collections.<Document>emptyList(), 0);
            }
            return new QueryResponse(getQueryStore().getAll(parameter.getTable(), ids, true), searchHits.totalHits());
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }
}
