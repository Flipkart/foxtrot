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
package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:51 PM
 */
public class QueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class.getSimpleName());

    private final AnalyticsLoader analyticsLoader;
    private final ExecutorService executorService;

    public QueryExecutor(AnalyticsLoader analyticsLoader, ExecutorService executorService) {
        this.analyticsLoader = analyticsLoader;
        this.executorService = executorService;
    }

    public <T extends ActionRequest> ActionResponse execute(T request) throws QueryStoreException {
        Action action = resolve(request);
        return action.execute();
    }

    public <T extends ActionRequest> AsyncDataToken executeAsync(T request) throws QueryStoreException {
        return resolve(request).execute(executorService);
    }

    public <T extends ActionRequest> Action resolve(T request) throws QueryStoreException {
        Action action;
        try {
            action = analyticsLoader.getAction(request);
        } catch (Exception e) {
            logger.error("Error resolving action: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.ACTION_RESOLUTION_ERROR,
                    "Error resolving action for: " + request.getClass().getCanonicalName(), e);
        }
        if (null == action) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.UNRESOLVABLE_OPERATION,
                    "No resolvable action could be found for: " + request.getClass().getCanonicalName());
        }
        return action;
    }

}
