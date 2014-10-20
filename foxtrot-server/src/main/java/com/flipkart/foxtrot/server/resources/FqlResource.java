package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.sql.QueryTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

@Produces({
        MediaType.APPLICATION_JSON
})
@Consumes({
        MediaType.TEXT_PLAIN
})
@Path("/v1/fql")
public class FqlResource {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsResource.class);
    private final QueryExecutor queryExecutor;

    public FqlResource(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @POST
    public ActionResponse runFql(final String query) {
        ActionRequest request = null;
        try {
            request = new QueryTranslator().translate(query);
            return queryExecutor.execute(request);
        } catch (QueryStoreException e) {
            logger.error(String.format("Error running sync request %s", request), e);
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        } catch (Exception e) {
            logger.error("Error running FQL query: " + e.getMessage(), e);
            throw new WebApplicationException(
                    Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        }
    }
}
