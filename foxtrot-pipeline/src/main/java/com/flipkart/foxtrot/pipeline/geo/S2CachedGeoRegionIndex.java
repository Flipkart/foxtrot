package com.flipkart.foxtrot.pipeline.geo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.SneakyThrows;
import org.geojson.Feature;

import java.io.IOException;
import java.util.List;

public class S2CachedGeoRegionIndex implements GeoRegionIndex {

    private GeoRegionIndex delegate;
    private int s2CellLevel;
    private Cache<Long, List<Feature>> cache;

    public S2CachedGeoRegionIndex(GeoRegionIndex delegate,
                                  String spec,
                                  int s2CellLevel) {
        this.delegate = delegate;
        this.s2CellLevel = s2CellLevel;
        cache = CacheBuilder.from(spec)
                .build();
    }

    @SneakyThrows
    @Override
    public List<Feature> matchingIds(GeoPoint point) {
        return cache.get(GeoUtils.toS2CellId(point, s2CellLevel), () -> delegate.matchingIds(point));
    }

    @Override
    public void index(List<Feature> geoJsonFeatures) throws IOException {
        delegate.index(geoJsonFeatures);
    }
}
