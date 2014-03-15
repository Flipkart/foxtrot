package com.flipkart.foxtrot.core.datastore;

import com.flipkart.foxtrot.common.Document;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:17 PM
 */
public interface DataStore {
    public void save(final String table, final Document document) throws DataStoreException;
    public void save(final String table, final List<Document> documents) throws DataStoreException;
    public Document get(final String table, final String id) throws DataStoreException;
    public List<Document> get(final String table, final List<String> ids) throws DataStoreException;
}
