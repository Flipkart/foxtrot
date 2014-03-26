package com.flipkart.foxtrot.core.querystore.actions.spi;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.common.Action;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 1:37 AM
 */
public class ActionMetadata {
    private final Class<? extends ActionRequest> request;
    private final Class<? extends Action> action;
    private final boolean cacheable;
    private final String cacheToken;

    public ActionMetadata(Class<? extends ActionRequest> request,
                          Class<? extends Action> action,
                          boolean cacheable,
                          String cacheToken) {
        this.request = request;
        this.action = action;
        this.cacheable = cacheable;
        this.cacheToken = cacheToken;
    }

    public Class<? extends ActionRequest> getRequest() {
        return request;
    }

    public Class<? extends Action> getAction() {
        return action;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public String getCacheToken() {
        return cacheToken;
    }
}
