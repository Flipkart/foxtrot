package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 13/12/15.
 */
public abstract class FoxtrotException extends Exception {

    private ErrorCode code;

    public FoxtrotException(ErrorCode code) {
        this.code = code;
    }

    public FoxtrotException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public FoxtrotException(ErrorCode code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public FoxtrotException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public abstract Map<String, Object> toMap();

    public ErrorCode getCode() {
        return code;
    }

    public void setCode(ErrorCode code) {
        this.code = code;
    }

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
