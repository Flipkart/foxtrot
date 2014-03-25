package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.query.CachableResponseGenerator;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 6:17 PM
 */
public abstract class ElasticsearchAction<RequestType extends CachableResponseGenerator, ReturnType>
                                                                extends Action<RequestType, ReturnType> {
    private DataStore dataStore;
    private ElasticsearchConnection connection;

    protected ElasticsearchAction(RequestType parameter) {
        super(parameter);
    }

    protected ElasticsearchAction(RequestType parameter, DataStore dataStore, ElasticsearchConnection connection) {
        super(parameter);
        this.dataStore = dataStore;
        this.connection = connection;
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public ElasticsearchConnection getConnection() {
        return connection;
    }

    public void setConnection(ElasticsearchConnection connection) {
        this.connection = connection;
    }
}
