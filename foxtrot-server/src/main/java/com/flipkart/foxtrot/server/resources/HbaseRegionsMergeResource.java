package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.hbase.HRegionData;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseRegions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.hbase.TableName;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.Min;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/v1/hbase/regions")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/hbase/regions")
@Singleton
@Order(20)
public class HbaseRegionsMergeResource {

    private HbaseRegions hbaseRegions;

    @Inject
    public HbaseRegionsMergeResource(HbaseConfig hbaseConfig) {
        this.hbaseRegions = new HbaseRegions(hbaseConfig);
    }

    @GET
    @Path("/{table}/{threshSizeInGB}/list")
    @Timed
    @ApiOperation("Get all Hbase regions which can be merged")
    public Map<String, List<List<HRegionData>>> listMergableRegions(@PathParam("table") final String tableName,
                                                                     @PathParam("threshSizeInGB") @Min(0) final double threshSizeInGB) {
        return Collections.singletonMap("regions", hbaseRegions.getMergeableRegions(TableName.valueOf(tableName), threshSizeInGB));
    }

    @GET
    @Path("/{table}/{threshSizeInGB}/merge/{number}")
    @Timed
    @ApiOperation("Merge Hbase regions")
    public Response mergeRegions(@PathParam("table") final String tableName,
                                 @PathParam("threshSizeInGB") @Min(0) final double threshSizeInGB,
                                 @PathParam("number") @Min(-1) final int number) {
        hbaseRegions.mergeRegions(TableName.valueOf(tableName), threshSizeInGB, number);
        return Response.ok()
                .build();
    }
}
