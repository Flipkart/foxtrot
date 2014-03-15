package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Query;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:25 PM
 */
public interface QueryStore {
    public void save(final String table, final Document document) throws QueryStoreException;
    public void save(final String table, final List<Document> document) throws QueryStoreException;
    public Document get(final String table, final String id) throws QueryStoreException;
    public List<Document> get(final String table, final List<String> ids) throws QueryStoreException;
    public List<Document> runQuery(final Query query) throws QueryStoreException;
    public String runQueryAsync(Query query) throws QueryStoreException;
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException;
}
