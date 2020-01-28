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
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
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

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com) Date: 15/03/14 Time: 10:55 PM
 */
@Path("/v1/document/{table}")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/document/{table}")
public class DocumentResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentResource.class);
    private static final String EVENT_TYPE = "eventType";
    private final QueryStore queryStore;
    private final Map<String, Map<String, List<String>>> tableEventConfigs;
    private final List<String> tablesToBeDuplicated;

    public DocumentResource(QueryStore queryStore, SegregationConfiguration segregationConfiguration) {
        this.queryStore = queryStore;
        this.tableEventConfigs = segregationConfiguration.getTableEventConfigs();
        this.tablesToBeDuplicated = segregationConfiguration.getTablesToBeDuplicated();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @ApiOperation("Save Document")
    public Response saveDocument(@PathParam("table") String table, @Valid final Document document) {
        String tableName = preProcess(table, document);
        if (tableName != null) {
            queryStore.save(tableName, document);
        }
        if (tableName != null && !table.equals(tableName) && tablesToBeDuplicated != null &&
                tablesToBeDuplicated.contains(tableName)) {
            queryStore.save(table, document);
        }
        return Response.created(URI.create("/" + document.getId()))
                .build();
    }

    private String preProcess(String table, Document document) {
        if (document.getData()
                .has(EVENT_TYPE)) {
            String eventType = document.getData()
                    .get(EVENT_TYPE)
                    .asText();
            return getSegregatedTableName(table, eventType);
        }
        return table;
    }

    private String getSegregatedTableName(String table, String eventType) {
        if (tableEventConfigs != null && tableEventConfigs.containsKey(table)) {
            String tableName = getTableForEventType(tableEventConfigs.get(table), eventType);
            if (tableName != null) {
                return tableName;
            }
        }
        return table;
    }

    private String getTableForEventType(Map<String, List<String>> tableNameVsEventTypes, String eventType) {
        for (Map.Entry<String, List<String>> entry : CollectionUtils.nullSafeSet(tableNameVsEventTypes.entrySet())) {
            for (String tempEventType : CollectionUtils.nullSafeList(entry.getValue())) {
                if (tempEventType.equals(eventType)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    @POST
    @Path("/bulk")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @ApiOperation("Save list of documents")
    public Response saveDocuments(@PathParam("table") String table, @Valid final List<Document> documents) {
        Map<String, List<Document>> tableVsDocuments = preProcessSaveDocuments(table, documents);
        for (Map.Entry<String, List<Document>> entry : CollectionUtils.nullSafeSet(tableVsDocuments.entrySet())) {
            queryStore.save(entry.getKey(), entry.getValue());
            if (!entry.getKey()
                    .equals(table) && tablesToBeDuplicated != null && tablesToBeDuplicated.contains(entry.getKey())) {
                queryStore.save(table, entry.getValue());
            }
        }
        return Response.created(URI.create("/" + table))
                .build();
    }

    private Map<String, List<Document>> preProcessSaveDocuments(String table, List<Document> documents) {
        Map<String, List<Document>> tableVsDocuments = new HashMap<>();
        if (tableEventConfigs != null && tableEventConfigs.containsKey(table)) {
            for (Document document : CollectionUtils.nullSafeList(documents)) {
                String tableName = table;
                if (document.getData()
                        .has(EVENT_TYPE)) {
                    String eventType = document.getData()
                            .get(EVENT_TYPE)
                            .asText();
                    tableName = getSegregatedTableName(table, eventType);
                }
                if (tableVsDocuments.containsKey(tableName)) {
                    tableVsDocuments.get(tableName)
                            .add(document);
                }
                else {
                    List<Document> tableDocuments = Lists.newArrayList(document);
                    tableVsDocuments.put(tableName, tableDocuments);
                }
            }
        }
        else {
            tableVsDocuments.put(table, documents);
        }
        return tableVsDocuments;
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
    public Response getDocuments(
            @PathParam("table") final String table,
            @QueryParam("id") @NotNull final List<String> ids) {
        return Response.ok(queryStore.getAll(table, ids))
                .build();
    }

}
