package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.google.common.annotations.VisibleForTesting;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
@VisibleForTesting
@AnalyticsProvider(opcode = "testCacheableFalse", request = NonCacheableActionRequest.class, response = NonCacheableActionResponse.class, cacheable = false)
public class NonCacheableAction extends Action<NonCacheableActionRequest> {

    public NonCacheableAction(NonCacheableActionRequest parameter,
                              DataStore dataStore,
                              ElasticsearchConnection connection,
                              String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        return "TEST_CACHE_KEY";
    }

    @Override
    public ActionResponse execute(NonCacheableActionRequest parameter) throws QueryStoreException {
        return new NonCacheableActionResponse();
    }
}
