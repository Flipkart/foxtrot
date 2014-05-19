package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Path("/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableMetadataResource {
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataResource.class);
    private final TableMetadataManager tableMetadataManager;

    public TableMetadataResource(TableMetadataManager tableMetadataManager) {
        this.tableMetadataManager = tableMetadataManager;
    }

    @POST
    public Table save(@Valid final Table table) {
        try {
            tableMetadataManager.save(table);
        } catch (Exception e) {
            logger.error(String.format("Unable to save table %s", table), e);
            throw new WebApplicationException(Response.serverError().entity(Collections.singletonMap("error", e.getMessage())).build());
        }
        return table;
    }

    @GET
    @Path("/{name}")
    public Table get(@PathParam("name") final String name) throws Exception {
        Table table = tableMetadataManager.get(name);
        if (table == null) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }
        return table;
    }

    @GET
    public List<Table> get() throws Exception {
        return tableMetadataManager.get();
    }
}
