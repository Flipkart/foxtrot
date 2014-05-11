package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.TableFieldMapping;

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

    public TableFieldMapping getFieldMappings(final String table) throws QueryStoreException;
}
