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

import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
@Path("/v1/tables/{name}/fields")
@Produces(MediaType.APPLICATION_JSON)
public class TableFieldMappingResource {

    private QueryStore queryStore;

    public TableFieldMappingResource(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @GET
    public Response get(@PathParam("name") final String table) throws FoxtrotException {
        return Response.ok(queryStore.getFieldMappings(table)).build();
    }

}
