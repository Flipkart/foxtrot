package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Path("/foxtrot/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableMetadataResource {
    private final TableMetadataManager tableMetadataManager;

    public TableMetadataResource(TableMetadataManager tableMetadataManager) {
        this.tableMetadataManager = tableMetadataManager;
    }

    @POST
    public Table save(@Valid final Table table) {
        try {
            tableMetadataManager.save(table);
        } catch (Exception e) {
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
