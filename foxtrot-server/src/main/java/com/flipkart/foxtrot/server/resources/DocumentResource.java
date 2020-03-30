/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.server.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.TableTranslator;
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:55 PM
 */
@Path("/v1/document/{table}")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/document/{table}")
public class DocumentResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentResource.class);
    private static final String EVENT_TYPE = "eventType";
    private final QueryStore queryStore;
    private final TableTranslator tableTranslator;

    public DocumentResource(QueryStore queryStore, TableTranslator tableTranslator) {
        this.queryStore = queryStore;
        this.tableTranslator = tableTranslator;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @ApiOperation("Save Document")
    public Response saveDocument(@PathParam("table") String table, @Valid final Document document) {
        String tableName = tableTranslator.getTable(table, document);
        if(tableName != null) {
            queryStore.save(tableName, document);
        }
        if(tableName != null && !table.equals(tableName)) {
            queryStore.save(table, document);
        }
        return Response.created(URI.create("/" + document.getId()))
                .build();
    }

    @POST
    @Path("/bulk")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @ApiOperation("Save list of documents")
    public Response saveDocuments(@PathParam("table") String table, @Valid final List<Document> documents) {
        Map<String, List<Document>> tableVsDocuments = getTableVsDocuments(table, documents);
        for(Map.Entry<String, List<Document>> entry : CollectionUtils.nullSafeSet(tableVsDocuments.entrySet())) {
            queryStore.save(entry.getKey(), entry.getValue());
        }
        return Response.created(URI.create("/" + table))
                .build();
    }

    @GET
    @Path("/{id}")
    @Timed
    @ApiOperation("Get Document")
    public Response getDocument(@PathParam("table") final String table, @PathParam("id") @NotNull final String id) {
        return Response.ok(queryStore.get(table, id))
                .build();
    }

    @GET
    @Timed
    @ApiOperation("Get Documents")
    public Response getDocuments(@PathParam("table") final String table, @QueryParam("id") @NotNull final List<String> ids) {
        return Response.ok(queryStore.getAll(table, ids))
                .build();
    }


    private Map<String, List<Document>> getTableVsDocuments(String table, List<Document> documents) {
        Map<String, List<Document>> tableVsDocuments = new HashMap<>();
        if(tableTranslator.isTransformableTable(table)) {
            for(Document document : CollectionUtils.nullSafeList(documents)) {
                String tableName = tableTranslator.getTable(table, document);

                if(tableVsDocuments.containsKey(tableName)) {
                    tableVsDocuments.get(tableName)
                            .add(document);
                } else {
                    List<Document> tableDocuments = Lists.newArrayList(document);
                    tableVsDocuments.put(tableName, tableDocuments);
                }
            }
        } else {
            tableVsDocuments.put(table, documents);
        }
        return tableVsDocuments;
    }

}
