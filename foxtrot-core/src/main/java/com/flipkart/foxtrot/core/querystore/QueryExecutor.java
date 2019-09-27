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
import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:51 PM
 */
@Slf4j
public class QueryExecutor {

    private final AnalyticsLoader analyticsLoader;
    private final ExecutorService executorService;
    private final List<ActionExecutionObserver> executionObservers;

    public QueryExecutor(
            AnalyticsLoader analyticsLoader,
            ExecutorService executorService,
            List<ActionExecutionObserver> executionObservers) {
        this.analyticsLoader = analyticsLoader;
        this.executorService = executorService;
        this.executionObservers = executionObservers;
    }

    public <T extends ActionRequest> ActionValidationResponse validate(T request, String email) {
        return resolve(request).validate(email);
    }

    public <T extends ActionRequest> ActionResponse execute(T request, String email) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Action action = null;
        ActionEvaluationResponse evaluationResponse = null;
        try {
            action = resolve(request);
            action.preProcessRequest(email);
            final ActionResponse cachedData = readCachedData(analyticsLoader.getCacheManager(), request, action);
            if (cachedData != null) {
                cachedData.setFromCache(true);
                evaluationResponse = ActionEvaluationResponse.success(
                        action, request, cachedData, stopwatch.elapsed(TimeUnit.MILLISECONDS), true);
                return cachedData;
            }
            notifyObserverPreExec(request);
            final ActionResponse response = action.execute();
            evaluationResponse = ActionEvaluationResponse.success(
                    action, request, response, stopwatch.elapsed(TimeUnit.MILLISECONDS), false);
            return response;

        } catch (FoxtrotException e) {
            long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            log.info("Elapsed time in query execution: {}, request: {}, Error: {}", elapsedTime, request, e);
            evaluationResponse = ActionEvaluationResponse.failure(
                    action, request, e, elapsedTime);
            throw e;
        }
        finally {
            notifyObserverPostExec(evaluationResponse);
        }
    }

    public <T extends ActionRequest> AsyncDataToken executeAsync(T request, String email) {
        final Action action = resolve(request);
        final String cacheKey = action.cacheKey();
        final AsyncDataToken dataToken = new AsyncDataToken(request.getOpcode(), cacheKey);
        final ActionResponse response = readCachedData(analyticsLoader.getCacheManager(), request, action);
        if(null != response) {
            // If data exists in the cache nothing to do.. just return
            return dataToken;
        }
        //Otherwise schedule
        executorService.submit(() -> {
            final ActionResponse execute = execute(request, email);
            analyticsLoader.getCacheManager().getCacheFor(dataToken.getAction())
                    .put(dataToken.getKey(), execute);
        });
        return dataToken;
    }

    public <T extends ActionRequest> Action resolve(T request) {
        Action action;
        action = analyticsLoader.getAction(request);
        if (null == action) {
            throw FoxtrotExceptions.createUnresolvableActionException(request);
        }
        return action;
    }

    private void notifyObserverPreExec(final ActionRequest request) {
        if(null == executionObservers) {
            return;
        }
        executionObservers
                .forEach(actionExecutionObserver -> actionExecutionObserver.preExecution(request));
    }

    private void notifyObserverPostExec(final ActionEvaluationResponse evaluationResponse) {
        if(null == executionObservers) {
            return;
        }
        executionObservers
                .forEach(actionExecutionObserver -> actionExecutionObserver.postExecution(evaluationResponse));
    }

    private ActionResponse readCachedData(final CacheManager cacheManager,
                                          final ActionRequest request,
                                          final Action action) {
        final Cache cache = cacheManager.getCacheFor(request.getOpcode());
        if (null != cache) {
            final String cacheKey = action.cacheKey();
            if (cache.has(cacheKey)) {
                log.info("Cache hit for key: {}", cacheKey);
                return cache.get(cacheKey);
            }
            else {
                log.info("Cache miss for key: {}", cacheKey);
            }
        }
        return null;
    }
}
