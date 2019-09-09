/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.common.noncacheable;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.action.search.SearchRequestBuilder;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
@VisibleForTesting
@AnalyticsProvider(opcode = "no-cache-test", request = NonCacheableActionRequest.class, response = NonCacheableActionResponse.class,
                   cacheable = false)
public class NonCacheableAction extends Action<NonCacheableActionRequest> {

    public NonCacheableAction(NonCacheableActionRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
    }

    @Override
    public void preprocess() {

    }

    @Override
    public String getMetricKey() {
        return "TEST";
    }

    @Override
    public String getRequestCacheKey() {
        return "TEST_CACHE_KEY";
    }

    @Override
    public void validateImpl(NonCacheableActionRequest parameter) throws MalformedQueryException {

    }

    @Override
    public SearchRequestBuilder getRequestBuilder(NonCacheableActionRequest parameter) throws FoxtrotException {
        return null;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, NonCacheableActionRequest parameter)
            throws FoxtrotException {
        return null;
    }

    @Override
    public ActionResponse execute(NonCacheableActionRequest parameter) throws FoxtrotException {
        return new NonCacheableActionResponse();
    }
}
