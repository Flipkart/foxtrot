package com.flipkart.foxtrot.pipeline.geo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import lombok.SneakyThrows;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

public class ESRIGeoRegionIndexTest extends TestCase {

    private String textPolygons = "{\"type\": \"FeatureCollection\",\"features\":["
            + "{\"type\": \"Feature\",\"properties\": {\"id\":1},\"geometry\": {\"type\": \"Polygon\",\"coordinates\": [[[86.15753173828125,24.906367237907997],[86.7919921875,24.906367237907997],[86.7919921875,25.42591200329217],[86.15753173828125,25.42591200329217],[86.15753173828125,24.906367237907997]]]}},\n"
            + "{\"type\": \"Feature\",\"properties\": {\"id\":2},\"geometry\": {\"type\": \"Polygon\",\"coordinates\": [[[86.45965576171875,24.612063338963782],[87.21221923828125,24.612063338963782],[87.21221923828125,25.209911213827688],[86.45965576171875,25.209911213827688],[86.45965576171875,24.612063338963782]]]}},\n"
            + "{\"type\": \"Feature\",\"properties\": {\"id\":3},\"geometry\": {\"type\": \"Polygon\",\"coordinates\": [[[85.93780517578125,24.427145340082046],[87.43194580078124,24.427145340082046],[87.43194580078124,25.723209559418265],[85.93780517578125,25.723209559418265],[85.93780517578125,24.427145340082046]]]}}]}";
    private String polygonWithHoles = "{\"type\": \"FeatureCollection\",\"features\":[{\"type\": \"Feature\",\"properties\": {},\"geometry\": {\"type\": \"Polygon\",\"coordinates\": [[[76.87133789062501,18.33366944577131],[79.45861816406251,18.33366944577131],[79.45861816406251,19.761533975023298],[76.87133789062501,19.761533975023298],[76.87133789062501,18.33366944577131]],[[77.65686035156251,18.70869162255995],[78.55224609375,18.70869162255995],[78.55224609375,19.36297613334182],[77.65686035156251,19.36297613334182],[77.65686035156251,18.70869162255995]]]}}]}";

    private String multiPolygon = "{\"type\": \"FeatureCollection\",\"features\":[{\"type\": \"Feature\",\"properties\": {},\"geometry\": {\"type\": \"MultiPolygon\",\"coordinates\": [[[[76.87133789062501,18.33366944577131],[79.45861816406251,18.33366944577131],[79.45861816406251,19.761533975023298],[76.87133789062501,19.761533975023298],[76.87133789062501,18.33366944577131]]],[[[77.47009277343751,20.488773287109822],[78.365478515625,20.488773287109822],[78.365478515625,21.135745255030578],[77.47009277343751,21.135745255030578],[77.47009277343751,20.488773287109822]]]]}}]}";

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @SneakyThrows
    private List<Feature> getFeatures(String text) {
        return new ObjectMapper().readValue(text, FeatureCollection.class)
                .getFeatures();
    }

    @Test
    public void testMatchingIdsPolygons() throws JsonProcessingException {
        Stream.of(
                new S2CachedGeoRegionIndex(new ESRIGeoRegionIndex(new ObjectMapper()), "", 20))
                .forEach(index -> {
                    indexDocuments(index, textPolygons);

                    assertEquals(0, index.matchingIds(new GeoPoint(25.82956108605351, 86.33056640625))
                            .size());
                    assertEquals(1, index.matchingIds(new GeoPoint(25.611809521055477, 86.7755126953125))
                            .size());
                    assertEquals(2, index.matchingIds(new GeoPoint(25.331614221401054, 86.253662109375))
                            .size());
                    assertEquals(2, index.matchingIds(new GeoPoint(24.84656534821976, 87.000732421875))
                            .size());
                    assertEquals(3, index.matchingIds(new GeoPoint(25.05076877966861, 86.6162109375))
                            .size());
                });

    }

    @SneakyThrows
    private void indexDocuments(GeoRegionIndex index,
                                String textPolygons) {
        index.index(getFeatures(textPolygons));
    }

    @Test
    public void testMatchingIdsPolygonWithHoles() throws JsonProcessingException {

        Stream.of(new ESRIGeoRegionIndex(new ObjectMapper()),
                new S2CachedGeoRegionIndex(new ESRIGeoRegionIndex(new ObjectMapper()), "", 20))
                .forEach(index -> {

                    indexDocuments(index, polygonWithHoles);
                    assertEquals(0, index.matchingIds(new GeoPoint(18.999802829053262, 78.134765625))
                            .size());
                    assertEquals(1, index.matchingIds(new GeoPoint(19.316327373141174, 79.0850830078125))
                            .size());
                    assertEquals(0, index.matchingIds(new GeoPoint(20.11268153468498, 78.189697265625))
                            .size());
                });
    }

    @Test
    public void testMatchingIdsWithMultiPolygon() throws JsonProcessingException {
        Stream.of(new ESRIGeoRegionIndex(new ObjectMapper()),
                new S2CachedGeoRegionIndex(new ESRIGeoRegionIndex(new ObjectMapper()), "", 20))
                .forEach(index -> {

                    indexDocuments(index, multiPolygon);
                    assertEquals(1, index.matchingIds(new GeoPoint(19.316327373141174, 79.0850830078125))
                            .size());
                    assertEquals(0, index.matchingIds(new GeoPoint(20.226120295836992, 79.0191650390625))
                            .size());
                    assertEquals(1, index.matchingIds(new GeoPoint(20.673905264672843, 77.969970703125))
                            .size());
                });
    }
}