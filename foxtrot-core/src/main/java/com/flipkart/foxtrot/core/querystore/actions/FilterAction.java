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

import java.util.Collections;
import java.util.Vector;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 1:00 PM
 */
@AnalyticsProvider(opcode = "query", request = Query.class, response = QueryResponse.class, cacheable = false)
public class FilterAction extends Action<Query> {
    private static final Logger logger = LoggerFactory.getLogger(FilterAction.class);

    public FilterAction(Query parameter,
                        TableMetadataManager tableMetadataManager,
                        DataStore dataStore,
                        QueryStore queryStore,
                        ElasticsearchConnection connection,
                        String cacheToken) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken);
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
    public QueryResponse execute(Query parameter) throws QueryStoreException {
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
        SearchRequestBuilder search = null;
        SearchResponse response;
        try {
            /*if(!tableManager.exists(query.getTable())) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "There is no table called: " + query.getTable());
            }*/
            search = getConnection().getClient().prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setFrom(parameter.getFrom())
                    .setSize(parameter.getLimit());
            search.addSort(parameter.getSort().getField(),
                    ResultSort.Order.desc == parameter.getSort().getOrder() ? SortOrder.DESC : SortOrder.ASC);
            response = search.execute().actionGet();
            Vector<String> ids = new Vector<String>();
            SearchHits searchHits = response.getHits();
            for (SearchHit searchHit : searchHits) {
                ids.add(searchHit.getId());
            }
            if (ids.isEmpty()) {
                return new QueryResponse(Collections.<Document>emptyList(), 0);
            }
            return new QueryResponse(getQueryStore().getAll(parameter.getTable(), ids, true), searchHits.totalHits());
        } catch (Exception e) {
            if (null != search) {
                logger.error("Error running generated query: " + search, e);
            } else {
                logger.error("Query generation error: ", e);
            }
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running query: " + parameter.toString());
        }
    }
}
