package com.flipkart.foxtrot.server.resources;/**
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

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.CardinalityStatus;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationService;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/***
 Created by nitish.goyal on 17/08/18
 ***/
@Path("/v1/cardinality")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/cardinality")
@Singleton
@PermitAll
public class CardinalityCalculationResource {

    private final ExecutorService executorService;
    private final CardinalityCalculationService cardinalityCalculationService;
    private final CardinalityCalculationFactory cardinalityCalculationFactory;

    @Inject
    public CardinalityCalculationResource(final ExecutorService executorService,
                                          final CardinalityCalculationService cardinalityCalculationService,
                                          final CardinalityCalculationFactory cardinalityCalculationFactory) {
        this.executorService = executorService;
        this.cardinalityCalculationService = cardinalityCalculationService;
        this.cardinalityCalculationFactory = cardinalityCalculationFactory;
    }

    @Path("/calculate")
    @Timed
    @POST
    @ApiOperation("Update Cardinality Cache For All Tables")
    public Response updateCardinalityCache() {
        executorService.submit(cardinalityCalculationFactory.getRunnable());
        return Response.ok()
                .build();
    }

    @Path("/calculate/{table}")
    @Timed
    @POST
    @ApiOperation("Update Cardinality Cache For a Table")
    public Response updateCardinalityCache(@PathParam("table") final String table,
                                           @QueryParam("maxTimeToRunJob") @DefaultValue("60") int maxTimeToRunJobInMinutes,
                                           @QueryParam("retryCount") @DefaultValue("1") int retryCount) {
        executorService.submit(cardinalityCalculationFactory.getRunnable(maxTimeToRunJobInMinutes, retryCount, table));
        return Response.ok()
                .build();
    }

    @GET
    @Path("/audit/summary")
    @Timed
    @ApiOperation("Get Cardinality Audit Info Summary")
    public Response getCardinalityAuditInfoSummary(@QueryParam("details") boolean detailed) {
        return Response.ok()
                .entity(cardinalityCalculationService.fetchAuditSummary(detailed))
                .build();
    }

    @Path("/audit/reset/{table}")
    @Timed
    @POST
    @ApiOperation("Reset Cardinality Audit For a Table")
    public Response resetCardinalityAuditInfo(@PathParam("table") final String table) {
        cardinalityCalculationService.updateAuditInfo(ElasticsearchUtils.getValidName(table),
                CardinalityCalculationAuditInfo.builder()
                        .status(CardinalityStatus.PENDING)
                        .updatedAt(new Date())
                        .build());
        return Response.ok()
                .build();
    }

    @Path("/audit/reset")
    @Timed
    @POST
    @ApiOperation("Reset Cardinality Audit For Tables")
    public Response resetCardinalityAuditInfoForStatus(@QueryParam("status") final String status) {
        Map<String, CardinalityCalculationAuditInfo> auditInfoMap = cardinalityCalculationService.fetchAuditInfo();

        List<String> statusMatchingTables = auditInfoMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue()
                        .getStatus()
                        .equals(CardinalityStatus.valueOf(status)))
                .map(Entry::getKey)
                .collect(Collectors.toList());
        executorService.submit(() -> statusMatchingTables.forEach(
                table -> cardinalityCalculationService.updateAuditInfo(ElasticsearchUtils.getValidName(table),
                        CardinalityCalculationAuditInfo.builder()
                                .status(CardinalityStatus.PENDING)
                                .updatedAt(new Date())
                                .build())));
        return Response.accepted()
                .build();
    }
}
