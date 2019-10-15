package com.flipkart.foxtrot.core.querystore.handlers;

import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.querystore.ActionEvaluationResponse;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;

/**
 *
 */
public class ResponseCacheUpdater implements ActionExecutionObserver {

    private final CacheManager cacheManager;

    public ResponseCacheUpdater(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void postExecution(ActionEvaluationResponse response) {
        if (null == response.getResponse() || null == response.getExecutedAction()) {
            return;
        }
        final Cache cache = cacheManager.getCacheFor(response.getRequest().getOpcode());
        if (null == cache) {
            return;
        }
        final String cacheKey = response.getExecutedAction().cacheKey();
        cache.put(cacheKey, response.getResponse());
    }
}
