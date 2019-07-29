/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/
package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:51 PM
 */

public class QueryExecutor {

    private final AnalyticsLoader analyticsLoader;
    private final ExecutorService executorService;

    public QueryExecutor(AnalyticsLoader analyticsLoader, ExecutorService executorService) {
        this.analyticsLoader = analyticsLoader;
        this.executorService = executorService;
    }

    public <T extends ActionRequest> ActionValidationResponse validate(T request, String email) {
        return resolve(request).validate(email);
    }

    public <T extends ActionRequest> Action resolve(T request) {
        Action action;
        action = analyticsLoader.getAction(request);
        if (null == action) {
            throw FoxtrotExceptions.createUnresolvableActionException(request);
        }
        return action;
    }

    public <T extends ActionRequest> ActionResponse execute(T request, String email) {
        return resolve(request).execute(email);
    }

    public <T extends ActionRequest> AsyncDataToken executeAsync(T request, String email) {
        return resolve(request).execute(executorService, email);
    }

}
