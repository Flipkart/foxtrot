package com.flipkart.foxtrot.pipeline.geo;

import com.esri.core.geometry.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import lombok.val;
import org.geojson.Feature;
import org.geojson.LngLatAlt;
import org.geojson.MultiPolygon;
import org.geojson.Polygon;

import java.util.ArrayList;
import java.util.List;

public class ESRIGeoRegionIndex implements GeoRegionIndex {

    private QuadTree quadTree;
    private List<Geometry> geometries;
    private List<Feature> documents;
    private ObjectMapper objectMapper;
    private SpatialReference spatialReference;

    @Inject
    public ESRIGeoRegionIndex(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        geometries = new ArrayList<>();
        documents = new ArrayList<>();
        quadTree = new QuadTree(new Envelope2D(-180, -180, 180, 180), 8);
        spatialReference = SpatialReference.create(4326);
    }

    @Override
    public void index(List<Feature> geoJsonFeatures) throws JsonProcessingException {
        int i = 0;
        for (Feature feature : geoJsonFeatures) {
            if (feature.getGeometry() instanceof MultiPolygon) {
                for (List<List<LngLatAlt>> polygon : ((MultiPolygon) feature.getGeometry()).getCoordinates()) {
                    Polygon polyJson = new Polygon();
                    polygon.forEach(polyJson::add);
                    val geojson = objectMapper.writeValueAsString(polyJson);
                    createEnvelope(i, feature, geojson);
                    i++;
                }
            }
            if (feature.getGeometry() instanceof Polygon) {
                val geojson = objectMapper.writeValueAsString(feature.getGeometry());
                createEnvelope(i, feature, geojson);
                i++;
            }
        }
    }

    @Override
    public List<Feature> matchingIds(GeoPoint point) {
        List<Feature> result = new ArrayList<>();
        Point esriPoint = GeoUtils.toPoint(point);
        val it = quadTree.getIterator(esriPoint, 0);

        int elmHandle = it.next();
        while (elmHandle >= 0) {
            int featureIndex = quadTree.getElement(elmHandle);
            if (GeometryEngine.contains(geometries.get(featureIndex), esriPoint, spatialReference)) {
                result.add(documents.get(featureIndex));
            }
            elmHandle = it.next();
        }
        return result;
    }

    private void createEnvelope(int i,
                                Feature doc,
                                String geojson) {
        val geometry = GeometryEngine.geoJsonToGeometry(geojson, 0, Geometry.Type.Unknown);
        Envelope envelope = new Envelope();
        geometry.getGeometry()
                .queryEnvelope(envelope);
        geometries.add(geometry.getGeometry());
        documents.add(doc);
        quadTree.insert(i,
                new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
    }
}
