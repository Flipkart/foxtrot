package com.flipkart.foxtrot.pipeline.geo;

import com.esri.core.geometry.Point;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import lombok.experimental.UtilityClass;

@UtilityClass
public class GeoUtils {

    public static Point toPoint(GeoPoint point) {
        return new Point(point.getLng(), point.getLat());
    }

    public static S2LatLng toS2LatLng(GeoPoint point) {
        return S2LatLng.fromDegrees(point.getLat(), point.getLng());
    }

    public static long toS2CellId(GeoPoint point, int level) {
        return S2CellId.fromLatLng(toS2LatLng(point)).parent(level).id();
    }

}
