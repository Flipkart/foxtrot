/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/util")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/util")
@Singleton
@PermitAll
public class UtilResource {

    private final ElasticsearchConfig elasticsearch;
    private final ObjectMapper mapper;

    @Inject
    public UtilResource(ElasticsearchConfig elasticsearch,
                        ObjectMapper mapper) {
        this.elasticsearch = elasticsearch;
        this.mapper = mapper;
    }

    @GET
    @Path("/config")
    @Timed
    @ApiOperation("Get config")
    public JsonNode configuration() {
        return mapper.createObjectNode()
                .set("elasticsearch", mapper.createObjectNode()
                        .put("tableNamePrefix", elasticsearch.getTableNamePrefix())
                        .set("hosts", mapper.valueToTree(elasticsearch.getHosts())));
    }
}
