/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Path("/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/tables/index/metadata")
@Singleton
@PermitAll
public class IndexMetadataResource {

    private final TableIndexMetadataService tableIndexMetadataService;
    private final ExecutorService executorService;

    @Inject
    public IndexMetadataResource(final TableIndexMetadataService tableIndexMetadataService,
                                 final ExecutorService executorService) {
        this.tableIndexMetadataService = tableIndexMetadataService;
        this.executorService = executorService;
    }

    @POST
    @Timed
    @Path("/index/metadata/sync")
    @ApiOperation("Sync Index Metadata")
    public Response syncMetadata(@QueryParam("syncDays") @DefaultValue("1") int syncDays) {
        executorService.submit(() -> tableIndexMetadataService.syncTableIndexMetadata(syncDays));
        return Response.status(Status.ACCEPTED)
                .build();
    }

    @POST
    @Timed
    @Path("/index/metadata/cleanup")
    @ApiOperation("Cleanup Index Metadata")
    public Response cleanupMetadata(@QueryParam("retentionDays") @DefaultValue("180") int retentionDays) {
        executorService.submit(() -> tableIndexMetadataService.cleanupIndexMetadata(retentionDays));
        return Response.status(Status.ACCEPTED)
                .build();
    }

    @GET
    @Timed
    @Path("/index/metadata/{indexName}")
    @ApiOperation("Get Index Metadata")
    public Response getIndexMetadata(@PathParam("indexName") String indexName) {
        TableIndexMetadata indexMetadata = tableIndexMetadataService.getIndexMetadata(indexName);
        return Response.ok()
                .entity(indexMetadata)
                .build();
    }

    @GET
    @Timed
    @Path("{table}/index/metadata")
    @ApiOperation("Get Table Indices Metadata")
    public Response getTableIndicesMetadata(@PathParam("table") String table) {
        List<TableIndexMetadata> indicesMetadata = tableIndexMetadataService.getTableIndicesMetadata(table);
        return Response.ok()
                .entity(indicesMetadata)
                .build();
    }

    @POST
    @Timed
    @Path("/index/metadata/query")
    @ApiOperation("Query Index Metadata")
    public Response getIndexMetadata(final List<Filter> filters) {
        List<TableIndexMetadata> indicesMetadata = tableIndexMetadataService.searchIndexMetadata(filters);
        return Response.ok()
                .entity(indicesMetadata)
                .build();
    }

}
