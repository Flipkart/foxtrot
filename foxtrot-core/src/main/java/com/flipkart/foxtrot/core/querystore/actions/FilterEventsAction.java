package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 1:00 PM
 */
@AnalyticsProvider(opcode = "query", request = Query.class, response = QueryResponse.class, cacheable = true)
public class FilterEventsAction extends Action<Query> {
    private static final Logger logger = LoggerFactory.getLogger(FilterEventsAction.class);

    public FilterEventsAction(Query parameter,
                              DataStore dataStore,
                              ElasticsearchConnection connection,
                              String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        Query query = getParameter();
        for(Filter filter : query.getFilters()) {
            filterHashKey += 31 * filter.hashCode();
        }

        return String.format("%s-%d-%d-%d", query.getTable(),
                                                        query.getFrom(), query.getLimit(), filterHashKey);
    }

    @Override
    public QueryResponse execute(Query parameter) throws QueryStoreException {
        SearchRequestBuilder search = null;
        try {
            /*if(!tableManager.exists(query.getTable())) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "There is no table called: " + query.getTable());
            }*/
            search = getConnection().getClient().prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable()))
                    .setTypes(ElasticsearchUtils.TYPE_NAME)
                    .setQuery(new ElasticSearchQueryGenerator(parameter.getCombiner()).genFilter(parameter.getFilters()))
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setFrom(parameter.getFrom())
                    .setSize(parameter.getLimit());
            if(null != parameter.getSort()) {
                search.addSort(parameter.getSort().getField(),
                        ResultSort.Order.desc == parameter.getSort().getOrder() ? SortOrder.DESC : SortOrder.ASC);
            }
            //logger.error("Running: " + search);
            SearchResponse response = search.execute().actionGet();
            Vector<String> ids = new Vector<String>();
            for(SearchHit searchHit : response.getHits()) {
                ids.add(searchHit.getId());
            }
            if(ids.isEmpty()) {
                return new QueryResponse(Collections.<Document>emptyList());
            }
            return new QueryResponse(getDataStore().get(parameter.getTable(), ids));
        } catch (Exception e) {
            if(null != search) {
                logger.error("Error running generated query: " + search);
            }
            else {
                logger.error("Query generation error: ", e);
            }
            try {
                throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                        "Error running query: " + ElasticsearchUtils.getMapper().writeValueAsString(parameter));
            } catch (JsonProcessingException e1) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_MALFORMED_QUERY_ERROR,
                        "Malformed query");
            }
        }
    }
}
