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

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
@Path("/v1/tables/{name}/fields")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {
    private static final Logger logger = LoggerFactory.getLogger(TableFieldMappingResource.class.getSimpleName());
    private QueryStore queryStore;

    public TableFieldMappingResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @GET
    public Response get(@PathParam("name") final String table) {
        try {
            return Response.ok(queryStore.getFieldMappings(table)).build();
        } catch (QueryStoreException ex) {
            logger.error("Unable to fetch Table Metadata " , ex);
            switch (ex.getErrorCode()) {
                case NO_SUCH_TABLE:
                    throw new WebApplicationException(
                            Response.status(Response.Status.NOT_FOUND).entity(Collections.singletonMap("error", ex.getMessage())).build());
                default:
                    throw new WebApplicationException(
                            Response.serverError().entity(Collections.singletonMap("error", "Metadata Fetch Failed")).build());
            }
        }
    }

}
