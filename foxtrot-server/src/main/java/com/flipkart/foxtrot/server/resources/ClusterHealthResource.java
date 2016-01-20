package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.flipkart.foxtrot.core.querystore.QueryStore;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

/**
 * Created by swapnil on 20/01/16.
 */

@Path("/v1/clusterhealth")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterHealthResource {
    private final QueryStore queryStore;
    private final ObjectMapper mapper;
    public ClusterHealthResource(QueryStore queryStore) {
        this.queryStore = queryStore;
        mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    @GET
    public String getHealth() throws ExecutionException, InterruptedException, JsonProcessingException {
        return mapper.writeValueAsString(queryStore.getClusterHealth());
    }
}
