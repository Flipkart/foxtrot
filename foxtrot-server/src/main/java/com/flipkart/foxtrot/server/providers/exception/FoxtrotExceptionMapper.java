package com.flipkart.foxtrot.server.providers.exception;

import com.flipkart.foxtrot.core.exception.FoxtrotException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class FoxtrotExceptionMapper implements ExceptionMapper<FoxtrotException> {

    @Override
    public Response toResponse(FoxtrotException e) {
        switch (e.getCode()) {
            case DOCUMENT_NOT_FOUND:
            case TABLE_NOT_FOUND:
                return Response.status(Response.Status.NOT_FOUND).entity(e).build();
            case STORE_CONNECTION_ERROR:
            case TABLE_INITIALIZATION_ERROR:
            case TABLE_METADATA_FETCH_FAILURE:
            case DATA_CLEANUP_ERROR:
            case STORE_EXECUTION_ERROR:
            case EXECUTION_EXCEPTION:
            case ACTION_EXECUTION_ERROR:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
            case MALFORMED_QUERY:
            case ACTION_RESOLUTION_FAILURE:
            case UNRESOLVABLE_OPERATION:
            case INVALID_REQUEST:
                return Response.status(Response.Status.BAD_REQUEST).entity(e).build();
            case TABLE_ALREADY_EXISTS:
                return Response.status(Response.Status.CONFLICT).entity(e).build();
            case CONSOLE_SAVE_EXCEPTION:
            case CONSOLE_FETCH_EXCEPTION:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
            default:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }
}
