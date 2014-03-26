package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.actions.QueryResponse;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:25 PM
 */
public interface QueryStore {
    public void save(final String table, final Document document) throws QueryStoreException;
    public void save(final String table, final List<Document> documents) throws QueryStoreException;
    public Document get(final String table, final String id) throws QueryStoreException;
    public List<Document> get(final String table, final List<String> ids) throws QueryStoreException;
    public ActionResponse runQuery(final Query query) throws QueryStoreException;
    public AsyncDataToken runQueryAsync(Query query) throws QueryStoreException;
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException;
    public HistogramResponse histogram(final HistogramRequest histogramRequest) throws QueryStoreException;
    public GroupResponse group(final GroupRequest groupRequest) throws QueryStoreException;
}
