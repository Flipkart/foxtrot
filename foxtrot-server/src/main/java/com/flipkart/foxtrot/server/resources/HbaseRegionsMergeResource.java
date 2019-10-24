package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseRegions;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/v1/hbase/regions")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/hbase/regions")
public class HbaseRegionsMergeResource {

    private HbaseRegions hbaseRegions;

    public HbaseRegionsMergeResource(HbaseConfig hbaseConfig) {
        this.hbaseRegions = new HbaseRegions(hbaseConfig);
    }

    @GET
    @Path("/{table}")
    @Timed
    @ApiOperation("Get all Hbase regions which can be merged")
    public Map<String, List<HRegionInfo>> getAllRegions(@PathParam("table") final String tableName) {
        System.out.println(TableName.valueOf(tableName));
        return Collections.singletonMap("regions", hbaseRegions.getRegions(TableName.valueOf(tableName)));
    }
}
