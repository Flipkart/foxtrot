package com.flipkart.foxtrot.common.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.cardinality.ProbabilityCalculationResult;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.common.visitor.ConsoleIdVisitorAdapter;

import java.util.Collections;
import java.util.List;

/**
 * Created by rishabh.goyal on 03/01/16.
 */
public class FoxtrotExceptions {

    public static final String ERROR_DELIMITER = "&&&";

    private FoxtrotExceptions() {
    }

    public static TableInitializationException createTableInitializationException(Table table,
                                                                                  String message) {
        return new TableInitializationException(table.getName(), message);
    }

    public static TenantCreationException createTenantCreationException(String message) {
        return new TenantCreationException(message);
    }

    public static TableMissingException createTableMissingException(String table) {
        return new TableMissingException(table);
    }

    public static TenantMissingException createTenantMissingException(String tenant) {
        return new TenantMissingException(tenant);
    }

    public static StoreConnectionException createConnectionException(Table table,
                                                                     Exception e) {
        return new StoreConnectionException(table.getName(), e);
    }


    public static BadRequestException createBadRequestException(String entity,
                                                                String reason) {
        return createBadRequestException(entity, Collections.singletonList(reason));
    }

    public static BadRequestException createBadRequestException(String entity,
                                                                List<String> reasons) {
        return new BadRequestException(entity, reasons);
    }

    public static BadRequestException createBadRequestException(Table table,
                                                                Exception e) {
        return createBadRequestException(table.getName(), e);
    }

    public static BadRequestException createBadRequestException(Tenant tenant,
                                                                Exception e) {
        return createBadRequestException(tenant.getTenantName(), e);
    }

    public static BadRequestException createBadRequestException(String table,
                                                                Exception e) {
        return new BadRequestException(table, e);
    }

    public static MalformedQueryException createMalformedQueryException(ActionRequest actionRequest,
                                                                        List<String> reasons) {
        return new MalformedQueryException(actionRequest, reasons);
    }

    public static CardinalityOverflowException createCardinalityOverflow(ActionRequest actionRequest,
                                                                         String requestStr,
                                                                         String cacheKey,
                                                                         List<String> field,
                                                                         ProbabilityCalculationResult probability) {
        return new CardinalityOverflowException(actionRequest, requestStr, field,
                actionRequest.accept(new ConsoleIdVisitorAdapter()), cacheKey, probability);
    }

    public static DocumentMissingException createMissingDocumentException(Table table,
                                                                          String id) {
        return new DocumentMissingException(table.getName(), Collections.singletonList(id));
    }

    public static DocumentMissingException createMissingDocumentsException(Table table,
                                                                           List<String> ids) {
        return new DocumentMissingException(table.getName(), ids);
    }

    public static StoreExecutionException createExecutionException(String entity,
                                                                   Exception e) {
        return new StoreExecutionException(entity, e);
    }

    public static ActionExecutionException createQueryExecutionException(ActionRequest actionRequest,
                                                                         Exception e) {
        return new ActionExecutionException(actionRequest, e);
    }

    public static TableExistsException createTableExistsException(String table) {
        return new TableExistsException(table);
    }

    public static TenantExistsException createTenantExistsException(String tenantName) {
        return new TenantExistsException(tenantName);
    }

    public static DataCleanupException createDataCleanupException(String message,
                                                                  Exception e) {
        return new DataCleanupException(message, e);
    }

    public static ServerException createServerException(String message,
                                                        Exception e) {
        return new ServerException(message, e);
    }

    public static QueryCreationException queryCreationException(ActionRequest actionRequest,
                                                                Exception e) {
        return new QueryCreationException(actionRequest, e);
    }

    public static ActionResolutionException createActionResolutionException(ActionRequest actionRequest,
                                                                            Exception e) {
        return new ActionResolutionException(actionRequest, e);
    }

    public static UnresolvableActionException createUnresolvableActionException(ActionRequest actionRequest) {
        return new UnresolvableActionException(actionRequest);
    }

    public static AuthorizationException createAuthorizationException(ActionRequest actionRequest,
                                                                      Exception e) {
        return new AuthorizationException(actionRequest, e);
    }

    public static ConsoleQueryBlockedException createConsoleQueryBlockedException(ActionRequest actionRequest) {
        return new ConsoleQueryBlockedException(actionRequest);
    }

    public static FqlQueryBlockedException createFqlQueryBlockedException(String query) {
        return new FqlQueryBlockedException(query);
    }

    public static PipelineMissingException createPipelineMissingException(String name) {
        return new PipelineMissingException(name);
    }

    public static PipelineCreationException createPipelineCreationException(String exceptionMessage) {
        return new PipelineCreationException(exceptionMessage);
    }

    public static PipelineExistsException createPipelineExistsException(String name) {
        return new PipelineExistsException(name);
    }
}
