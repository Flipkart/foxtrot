package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.visitor.ActionRequestFilterVisitor;
import com.flipkart.foxtrot.common.visitor.ConsoleIdVisitorAdapter;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.events.model.query.FullTextSearchQueryEvent;
import com.flipkart.foxtrot.core.events.model.query.QueryTimeoutEvent;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessed;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class EventPublisherActionExecutionObserver implements ActionExecutionObserver {

    private final InternalEventBus eventBus;
    private final EventBusManager eventBusManager;
    private final List<String> timeoutExceptionMessages;

    public EventPublisherActionExecutionObserver(InternalEventBus eventBus,
                                                 EventBusManager eventBusManager,
                                                 QueryConfig queryConfig) {
        this.eventBus = eventBus;
        this.eventBusManager = eventBusManager;
        this.timeoutExceptionMessages = Objects.nonNull(queryConfig.getTimeoutExceptionMessages())
                ? queryConfig.getTimeoutExceptionMessages()
                : new ArrayList<>();
    }

    @Override
    public void postExecution(ActionEvaluationResponse response) {
        if (null == response) {
            return;
        }
        if (null != response.getException()) {
            eventBus.publish(new QueryProcessingError(response.getRequest(), response.getException()));

            if (isQueryTimedOut(response)) {
                eventBusManager.postEvent(buildQueryTimeoutEvent(response).toIngestionEvent());

            }
        } else {

            postFullTextSearchQueryEvent(response);

            eventBus.publish(
                    new QueryProcessed(response.getRequest(), response.getResponse(), response.getElapsedTime()));
        }
    }

    private boolean isQueryTimedOut(ActionEvaluationResponse response) {
        return response.getException()
                .getCause() instanceof IOException && Objects.nonNull(response.getException()
                .getCause()
                .getMessage()) && timeoutExceptionMessages.stream()
                .anyMatch(timeoutExceptionMessage -> response.getException()
                        .getCause()
                        .getMessage()
                        .contains(timeoutExceptionMessage));
    }

    private QueryTimeoutEvent buildQueryTimeoutEvent(ActionEvaluationResponse response) {
        return QueryTimeoutEvent.builder()
                .actionRequest(JsonUtils.toJson(response.getRequest()))
                .cacheKey(response.getExecutedAction()
                        .getRequestCacheKey())
                .opCode(response.getRequest()
                        .getOpcode())
                .requestTags(response.getRequest()
                        .getRequestTags())
                .consoleId(response.getRequest()
                        .accept(new ConsoleIdVisitorAdapter()))
                .table(response.getRequest()
                        .accept(new TableActionRequestVisitor()))
                .sourceType(response.getRequest()
                        .getSourceType())
                .build();
    }

    private void postFullTextSearchQueryEvent(ActionEvaluationResponse response) {
        Map<String, List<Filter>> tableVsFilters = response.getRequest()
                .accept(new ActionRequestFilterVisitor());

        if (tableVsFilters.values()
                .stream()
                .flatMap(Collection::stream)
                .anyMatch(filter -> Arrays.asList(FilterOperator.contains, FilterOperator.wildcard)
                        .contains(filter.getOperator()))) {
            Set<String> filters = tableVsFilters.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .map(Filter::getOperator)
                    .collect(Collectors.toSet());

            Set<String> fullTextSearchFields = tableVsFilters.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(filter -> Arrays.asList(FilterOperator.contains, FilterOperator.wildcard)
                            .contains(filter.getOperator()))
                    .map(Filter::getField)
                    .collect(Collectors.toSet());

            FullTextSearchQueryEvent fullTextSearchQueryEvent = FullTextSearchQueryEvent.builder()
                    .actionRequest(JsonUtils.toJson(response.getRequest()))
                    .cacheKey(response.getExecutedAction()
                            .getRequestCacheKey())
                    .opCode(response.getRequest()
                            .getOpcode())
                    .requestTags(response.getRequest()
                            .getRequestTags())
                    .consoleId(response.getRequest()
                            .accept(new ConsoleIdVisitorAdapter()))
                    .table(response.getRequest()
                            .accept(new TableActionRequestVisitor()))
                    .sourceType(response.getRequest()
                            .getSourceType())
                    .elapsedTime(response.getElapsedTime())
                    .filters(filters)
                    .fullTextSearchFields(fullTextSearchFields)
                    .build();

            eventBusManager.postEvent(fullTextSearchQueryEvent.toIngestionEvent());

        }


    }
}
