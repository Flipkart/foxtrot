package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.*;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.core.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.val;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class GeoAggregationActionTest extends ActionTest {
    private static final String GEO_AGG_TABLE_NAME = "geo-agg-table";

    @Before
    public void setUp() throws Exception {
        super.setup();
        List<Document> documents = TestUtils.getGeoAggregationDocuments(getMapper());
        val table = Table.builder()
                .name(GEO_AGG_TABLE_NAME)
                .ttl(30)
                .seggregatedBackend(true)
                .shards(1)
                .columns(1000)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .customFieldMappings(new TreeMap<>(ImmutableMap.of("location", FieldDataType.GEOPOINT)))
                .build();
        tableMetadataManager.save(table);
        getQueryStore().initializeTable(table);
        getQueryStore().saveAll(GEO_AGG_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
    }

    @Test
    public void testCountLargeGrid() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setGridLevel(1);

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(1L, countResponse.getMetricByGrid().size());
        assertEquals(5L, countResponse.getMetricByGrid().entrySet().iterator().next().getValue());
    }

    @Test
    public void testCountSmallGrids() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setGridLevel(7);

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(5L, countResponse.getMetricByGrid().size());
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> assertEquals(1L, entry.getValue()));
    }

    @Test
    public void testCountSmallGridsWithFilter() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setGridLevel(7);
        geoAggregationRequest.setFilters(ImmutableList.of(new EqualsFilter("device", "nexus")));

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(3L, countResponse.getMetricByGrid().size());
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> assertEquals(1L, entry.getValue()));
    }

    @Test
    public void testUniqueCountSmallGridsWithFilter() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setUniqueCountOn("value");
        geoAggregationRequest.setGridLevel(7);
        geoAggregationRequest.setFilters(ImmutableList.of(new EqualsFilter("device", "nexus")));

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(3L, countResponse.getMetricByGrid().size());
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> assertEquals(1L, entry.getValue()));
    }

    @Test
    public void testCardinalityAggregationSmallGrids() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setAggregationField("device");
        geoAggregationRequest.setAggregationType(Stat.COUNT);
        geoAggregationRequest.setGridLevel(7);
        geoAggregationRequest.setFilters(ImmutableList.of(new EqualsFilter("device", "nexus")));

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(3L, countResponse.getMetricByGrid().size());
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> assertEquals(1L, entry.getValue()));
    }

    @Test
    public void testAggregationNumericSmallGrids() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setAggregationField("value");
        geoAggregationRequest.setAggregationType(Stat.MIN);
        geoAggregationRequest.setGridLevel(7);

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(5L, countResponse.getMetricByGrid().size());
        Map<Double, Integer> resultCount = new HashMap<>();
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> {
                    assertTrue((Double) entry.getValue() >= 1 && (Double) entry.getValue() <= 5);
                    assertFalse(resultCount.containsKey(entry.getValue()));
                    resultCount.put((Double) entry.getValue(), 1);
                });
    }


    @Test
    public void testCountLargeGridsWithAggregation() throws FoxtrotException {
        GeoAggregationRequest geoAggregationRequest = new GeoAggregationRequest();
        geoAggregationRequest.setTable(GEO_AGG_TABLE_NAME);
        geoAggregationRequest.setAggregationField("value");
        geoAggregationRequest.setAggregationType(Stat.SUM);
        geoAggregationRequest.setLocationField("location");
        geoAggregationRequest.setGridLevel(1);

        GeoAggregationResponse countResponse = GeoAggregationResponse.class.cast(getQueryExecutor().execute(geoAggregationRequest));

        assertNotNull(countResponse);
        assertEquals(1L, countResponse.getMetricByGrid().size());
        countResponse.getMetricByGrid().entrySet()
                .forEach(entry -> assertEquals(15.0, entry.getValue()));
    }

}
