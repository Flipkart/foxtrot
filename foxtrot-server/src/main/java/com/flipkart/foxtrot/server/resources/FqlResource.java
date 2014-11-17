package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.server.providers.FoxtrotExtraMediaType;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import net.sf.jsqlparser.JSQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, FoxtrotExtraMediaType.TEXT_CSV})
@Path("/v1/fql")
public class FqlResource {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsResource.class);
    //private final QueryExecutor queryExecutor;
    //private ObjectMapper objectMapper;
    private FqlEngine fqlEngine;

    public FqlResource(final FqlEngine fqlEngine) {
        this.fqlEngine = fqlEngine;
    }

    @POST
    public FlatRepresentation runFql(final String query) {
        try {
            return fqlEngine.parse(query);
        } catch (QueryStoreException e) {
            logger.error(String.format("Error running sync request %s", query), e);
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", getMessage(e))).build());
        } catch (Exception e) {
            /*if(null == request) {
                logger.error("Error running FQL query: " + e.getMessage(), e);
            }
            else {
                try {
                    logger.error("Error running FQL query: " + e.getMessage() + "[" + objectMapper.writeValueAsString(request) + "]", e);
                } catch (JsonProcessingException e1) {
                    logger.error("Error running FQL query: Could not parse to json: " + e.getMessage(), e);
                }
            }*/
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", getMessage(e))).build());
        }
    }

    String getMessage(Throwable e) {
        Throwable root = e;
        while (null != root.getCause()) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
