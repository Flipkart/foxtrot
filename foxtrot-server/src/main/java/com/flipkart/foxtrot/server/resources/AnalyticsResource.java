/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.google.common.base.Strings;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 2:05 AM
 */
@Path("/v1/analytics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AnalyticsResource {

    private final QueryExecutor queryExecutor;
    private final ObjectMapper objectMapper;

    public AnalyticsResource(QueryExecutor queryExecutor,
                             ObjectMapper objectMapper) {
        this.queryExecutor = queryExecutor;
        this.objectMapper = objectMapper;
    }

    @POST
    public ActionResponse runSync(final ActionRequest request) throws FoxtrotException {
        return queryExecutor.execute(request);
    }

    @POST
    @Path("/stream")
    public Response runStream(final Query query) throws FoxtrotException {
        StreamingOutput stream = os -> {
            try {
                JsonGenerator jsonGenerator = objectMapper.getFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                jsonGenerator.writeStartArray();

                query.setStreamEnabled(true);
                QueryResponse queryResponse = (QueryResponse) queryExecutor.execute(query);

                while (true) {
                    writeDocumentsToStream(jsonGenerator, queryResponse.getDocuments());
                    if (Strings.isNullOrEmpty(queryResponse.getStreamId())) {
                        break;
                    }
                    query.setStreamId(queryResponse.getStreamId());
                }

                jsonGenerator.writeEndArray();
                jsonGenerator.flush();
                jsonGenerator.close();
            } catch (FoxtrotException e) {
                throw new IOException(e);
            }
        };
        return Response.ok(stream).build();
    }

    @POST
    @Path("/async")
    public AsyncDataToken runSyncAsync(final ActionRequest request) throws FoxtrotException {
        return queryExecutor.executeAsync(request);
    }

    @POST
    @Path("/validate")
    public void validateQuery(final ActionRequest request) throws FoxtrotException {
        queryExecutor.validate(request);
    }

    private void writeDocumentsToStream(JsonGenerator jsonGenerator, List<Document> documents) throws IOException {
        if (documents == null || documents.isEmpty()) {
            return;
        }

        for (Document document : documents) {
            jsonGenerator.writeObject(document);
        }
    }
}
