/**
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

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:55 PM
 */
@Path("/v1/document/{table}")
@Produces(MediaType.APPLICATION_JSON)
public class DocumentResource {
    private static final Logger logger = LoggerFactory.getLogger(DocumentResource.class.getSimpleName());

    private final QueryStore queryStore;

    public DocumentResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Response saveDocument(@PathParam("table") final String table, @Valid final Document document) {
        try {
            queryStore.save(table, document);
        } catch (QueryStoreException ex) {
            logger.error("Save error : ", ex);
            switch (ex.getErrorCode()) {
                case NO_SUCH_TABLE:
                case INVALID_REQUEST:
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();
                case DOCUMENT_SAVE_ERROR:
                default:
                    return Response.serverError()
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();

            }
        }
        return Response.created(URI.create("/" + document.getId())).build();
    }

    @POST
    @Path("/bulk")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Response saveDocuments(@PathParam("table") final String table, @Valid final List<Document> document) {
        try {
            queryStore.save(table, document);
        } catch (QueryStoreException ex) {
            logger.error("Save error: ", ex);
            switch (ex.getErrorCode()) {
                case NO_SUCH_TABLE:
                case INVALID_REQUEST:
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();
                case DOCUMENT_SAVE_ERROR:
                default:
                    return Response.serverError()
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();

            }
        }
        return Response.created(URI.create("/" + table)).build();
    }

    @GET
    @Path("/{id}")
    @Timed
    public Response getDocument(@PathParam("table") final String table, @PathParam("id") @NotNull final String id) {
        try {
            return Response.ok(queryStore.get(table, id)).build();
        } catch (QueryStoreException ex) {
            logger.error("Get error : ", ex);
            switch (ex.getErrorCode()) {
                case DOCUMENT_NOT_FOUND:
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();
                case DOCUMENT_GET_ERROR:
                default:
                    return Response.serverError()
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();

            }
        }
    }

    @GET
    @Timed
    public Response getDocuments(@PathParam("table") final String table, @QueryParam("id") @NotNull final List<String> ids) {
        try {
            return Response.ok(queryStore.getAll(table, ids)).build();
        } catch (QueryStoreException ex) {
            logger.error("Get error : ", ex);
            switch (ex.getErrorCode()) {
                case DOCUMENT_NOT_FOUND:
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();
                case DOCUMENT_GET_ERROR:
                default:
                    return Response.serverError()
                            .entity(Collections.singletonMap("error", ex.getErrorCode()))
                            .build();

            }
        }
    }
}
