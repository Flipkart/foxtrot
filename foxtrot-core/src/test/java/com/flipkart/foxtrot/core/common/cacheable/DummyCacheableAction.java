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
package com.flipkart.foxtrot.core.common.cacheable;

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
@AnalyticsProvider(opcode = "cache-hit-test", request = DummyCacheableActionRequest.class, response = DummyCacheableActionResponse.class,
                   cacheable = false)
public class DummyCacheableAction extends Action<DummyCacheableActionRequest> {

    public DummyCacheableAction(DummyCacheableActionRequest parameter, AnalyticsLoader analyticsLoader) {
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
    public void validateImpl(DummyCacheableActionRequest parameter) throws MalformedQueryException {

    }

    @Override
    public SearchRequestBuilder getRequestBuilder(DummyCacheableActionRequest parameter) throws FoxtrotException {
        return null;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, DummyCacheableActionRequest parameter)
            throws FoxtrotException {
        return null;
    }

    @Override
    public ActionResponse execute(DummyCacheableActionRequest parameter) throws FoxtrotException {
        return new DummyCacheableActionResponse();
    }
}
