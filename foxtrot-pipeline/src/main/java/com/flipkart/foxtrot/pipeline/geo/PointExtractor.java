package com.flipkart.foxtrot.pipeline.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.val;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class PointExtractor {

    @SafeVarargs
    public static <T> Optional<T> firstPresent(final Supplier<Optional<T>>... optionals) {
        return Stream.of(optionals)
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());
    }

    public Optional<GeoPoint> extractFromRoot(JsonNode node) {
        if (node == null) {
            return Optional.empty();
        }
        return firstPresent(() -> extractAsLatitudeLongitude(node), () -> extractAsLatLng(node),
                () -> extractAsLatLon(node), () -> extractAsTwoDoubleArray(node));
    }

    private Optional<GeoPoint> extractAsLatLon(JsonNode node) {
        val latNode = node.get("lat");
        val lngNode = node.get("lon");
        if (latNode == null || lngNode == null || !latNode.isNumber() || !lngNode.isNumber()) {
            return Optional.empty();
        }
        return Optional.of(new GeoPoint(latNode.asDouble(), lngNode.asDouble()));
    }

    private Optional<GeoPoint> extractAsLatitudeLongitude(JsonNode node) {
        val latNode = node.get("latitude");
        val lngNode = node.get("longitude");
        if (latNode == null || lngNode == null || !latNode.isNumber() || !lngNode.isNumber()) {
            return Optional.empty();
        }
        return Optional.of(new GeoPoint(latNode.asDouble(), lngNode.asDouble()));
    }

    private Optional<GeoPoint> extractAsLatLng(JsonNode node) {
        val latNode = node.get("lat");
        val lngNode = node.get("lng");
        if (latNode == null || lngNode == null || !latNode.isNumber() || !lngNode.isNumber()) {
            return Optional.empty();
        }
        return Optional.of(new GeoPoint(latNode.asDouble(), lngNode.asDouble()));
    }

    private Optional<GeoPoint> extractAsTwoDoubleArray(JsonNode node) {
        if (node == null || !node.isArray()) {
            return Optional.empty();
        }
        val arrayNode = ((ArrayNode) node);
        if (arrayNode.size() != 2 || !arrayNode.get(0)
                .isNumber() || !arrayNode.get(1)
                .isNumber()) {
            return Optional.empty();
        }
        return Optional.of(new GeoPoint(arrayNode.get(1)
                .asDouble(), arrayNode.get(0)
                .asDouble()));
    }
}
