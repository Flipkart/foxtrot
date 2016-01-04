package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Table;

import java.util.Collections;
import java.util.List;

/**
 * Created by rishabh.goyal on 03/01/16.
 */
public class FoxtrotExceptions {

    public static TableInitializationException createTableInitializationException(Table table, String message) {
        return new TableInitializationException(table.getName(), message);
    }

    public static TableMissingException createTableMissingException(String table) {
        return new TableMissingException(table);
    }

    public static StoreConnectionException createConnectionException(Table table, Exception e) {
        return new StoreConnectionException(table.getName(), e);
    }


    public static BadRequestException createBadRequestException(String table, String reason) {
        return createBadRequestException(table, Collections.singletonList(reason));
    }

    public static BadRequestException createBadRequestException(String table, List<String> reasons) {
        return new BadRequestException(table, reasons);
    }

    public static BadRequestException createBadRequestException(Table table, Exception e) {
        return createBadRequestException(table.getName(), e);
    }

    public static BadRequestException createBadRequestException(String table, Exception e) {
        return new BadRequestException(table, e);
    }

    public static MalformedQueryException createMalformedQueryException(ActionRequest actionRequest, String reason) {
        return createMalformedQueryException(actionRequest, Collections.singletonList(reason));
    }

    public static MalformedQueryException createMalformedQueryException(ActionRequest actionRequest,
                                                                        List<String> reasons) {
        return new MalformedQueryException(actionRequest, reasons);
    }

    public static DocumentMissingException createMissingDocumentException(Table table, String id) {
        return new DocumentMissingException(table.getName(), Collections.singletonList(id));
    }

    public static DocumentMissingException createMissingDocumentsException(Table table, List<String> ids) {
        return new DocumentMissingException(table.getName(), ids);
    }

    public static StoreExecutionException createExecutionException(String table, Exception e) {
        return new StoreExecutionException(table, e);
    }

    public static ActionExecutionException createQueryExecutionException(ActionRequest actionRequest, Exception e) {
        return new ActionExecutionException(actionRequest, e);
    }

    public static TableExistsException createTableExistsException(String table) {
        return new TableExistsException(table);
    }

    public static DataCleanupException createDataCleanupException(String message, Exception e) {
        return new DataCleanupException(message, e);
    }

    public static QueryCreationException queryCreationException(ActionRequest actionRequest, Exception e) {
        return new QueryCreationException(actionRequest, e);
    }

    public static ActionResolutionException createActionResolutionException(ActionRequest actionRequest, Exception e) {
        return new ActionResolutionException(actionRequest, e);
    }

    public static UnresolvableActionException createUnresolvableActionException(ActionRequest actionRequest) {
        return new UnresolvableActionException(actionRequest);
    }
}
