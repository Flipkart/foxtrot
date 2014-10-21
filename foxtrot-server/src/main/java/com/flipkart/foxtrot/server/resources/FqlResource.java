package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.server.responseprocessors.FlatRepresentation;
import com.flipkart.foxtrot.server.responseprocessors.Flattener;
import com.flipkart.foxtrot.sql.QueryTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON })
@Consumes({MediaType.TEXT_PLAIN})
@Path("/v1/fql")
public class FqlResource {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsResource.class);
    private final QueryExecutor queryExecutor;
    private ObjectMapper objectMapper;

    public FqlResource(QueryExecutor queryExecutor, ObjectMapper objectMapper) {
        this.queryExecutor = queryExecutor;
        this.objectMapper = objectMapper;
    }

    @POST
    public FlatRepresentation runFql(final String query) {
        ActionRequest request = null;
        try {
            request = new QueryTranslator().translate(query);
            ActionResponse actionResponse = queryExecutor.execute(request);
            Flattener flattener = new Flattener(objectMapper, request);
            actionResponse.accept(flattener);
            return flattener.getFlatRepresentation();
        } catch (QueryStoreException e) {
            logger.error(String.format("Error running sync request %s", request), e);
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        } catch (Exception e) {
            if(null == request) {
                logger.error("Error running FQL query: " + e.getMessage(), e);
            }
            else {
                try {
                    logger.error("Error running FQL query: " + e.getMessage() + "[" + objectMapper.writeValueAsString(request) + "]", e);
                } catch (JsonProcessingException e1) {
                    logger.error("Error running FQL query: Could not parse to json: " + e.getMessage(), e);
                }
            }
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        }
    }
}
