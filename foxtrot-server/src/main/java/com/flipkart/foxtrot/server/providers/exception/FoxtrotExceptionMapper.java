package com.flipkart.foxtrot.server.providers.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class FoxtrotExceptionMapper implements ExceptionMapper<FoxtrotException> {

    private static final Logger logger = LoggerFactory.getLogger(FoxtrotExceptionMapper.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response toResponse(FoxtrotException e) {
        Map<String, Object> response = e.toMap();
        response.put("code", e.getCode());
        try {
            logger.error(mapper.writeValueAsString(response), e);
        } catch (JsonProcessingException e1) {
            logger.error("exception_serialization_failed", e1);
        }
        switch (e.getCode()) {
            case DOCUMENT_NOT_FOUND:
            case TABLE_NOT_FOUND:
                return Response.status(Response.Status.NOT_FOUND).entity(response).build();
            case STORE_CONNECTION_ERROR:
            case TABLE_INITIALIZATION_ERROR:
            case TABLE_METADATA_FETCH_FAILURE:
            case DATA_CLEANUP_ERROR:
            case STORE_EXECUTION_ERROR:
            case EXECUTION_EXCEPTION:
            case ACTION_EXECUTION_ERROR:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build();
            case MALFORMED_QUERY:
            case ACTION_RESOLUTION_FAILURE:
            case UNRESOLVABLE_OPERATION:
            case INVALID_REQUEST:
                return Response.status(Response.Status.BAD_REQUEST).entity(response).build();
            case TABLE_ALREADY_EXISTS:
                return Response.status(Response.Status.CONFLICT).entity(response).build();
            case CONSOLE_SAVE_EXCEPTION:
            case CONSOLE_FETCH_EXCEPTION:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build();
            default:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build();
        }
    }
}
