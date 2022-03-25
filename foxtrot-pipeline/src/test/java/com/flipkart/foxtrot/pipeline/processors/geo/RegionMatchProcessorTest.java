package com.flipkart.foxtrot.pipeline.processors.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.pipeline.geo.ESRIGeoRegionIndex;
import com.flipkart.foxtrot.pipeline.geo.GeoPoint;
import com.flipkart.foxtrot.pipeline.geo.PointExtractor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorInitializationException;
import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import lombok.val;
import org.geojson.Feature;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class RegionMatchProcessorTest {
    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        SerDe.init(mapper);
    }

    @Test(expected = ProcessorInitializationException.class)
    public void testInitializationException() throws IOException, ProcessorInitializationException {

        val def = new RegionMatchProcessorDefinition();
        val geoJsonReader = Mockito.spy(GeoJsonReader.class);
        doThrow(IOException.class).when(geoJsonReader).fetchFeatures(any());
        val underTest = new RegionMatchProcessor(def, new ESRIGeoRegionIndex(mapper), geoJsonReader, new PointExtractor(), false);
        underTest.init();

    }

    @Test
    public void testInitializationHappyPath() throws IOException, ProcessorInitializationException {
        val def = new RegionMatchProcessorDefinition();
        val geoJsonReader = Mockito.mock(GeoJsonReader.class);
        val underTest = new RegionMatchProcessor(def, new ESRIGeoRegionIndex(mapper), geoJsonReader, new PointExtractor(), false);
        when(geoJsonReader.fetchFeatures(anyString())).thenReturn(ImmutableList.of());
        underTest.init();
        underTest.init();
        verify(geoJsonReader, times(1)).fetchFeatures(any());
    }

    @Test
    public void testHandleWithFailingPointExtraction() throws IOException, ProcessorInitializationException {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });

        val def = RegionMatchProcessorDefinition.builder().matchField("$.point").targetFieldRootMapping(ImmutableMap.of("$.target", "name")).build();
        val geoJsonReader = Mockito.mock(GeoJsonReader.class);
        val pointExtractor = mock(PointExtractor.class);
        val underTest = new RegionMatchProcessor(def, new ESRIGeoRegionIndex(mapper), geoJsonReader, new PointExtractor(), false);
        when(geoJsonReader.fetchFeatures(anyString())).thenReturn(ImmutableList.of());
        underTest.init();
        when(pointExtractor.extractFromRoot(any(JsonNode.class))).thenReturn(Optional.empty());
        val data = "{\"point\":{\"lat\":12.77,\"lng\":77.45}}";
        val doc = Document.builder().data(mapper.readTree(data)).build();
        underTest.handle(doc);
        Assert.assertEquals(data, doc.getData().toString());
    }

    @Test
    public void testHandleWithNoMatchingFeatures() throws IOException, ProcessorInitializationException {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });

        val def = RegionMatchProcessorDefinition.builder().matchField("$.point").targetFieldRootMapping(ImmutableMap.of("$.target", "name")).build();

        val geoJsonReader = Mockito.mock(GeoJsonReader.class);
        val pointExtractor = mock(PointExtractor.class);
        val underTest = new RegionMatchProcessor(def, new ESRIGeoRegionIndex(mapper), geoJsonReader, new PointExtractor(), false);
        when(geoJsonReader.fetchFeatures(anyString())).thenReturn(ImmutableList.of());
        underTest.init();
        when(pointExtractor.extractFromRoot(any(JsonNode.class))).thenReturn(Optional.of(new GeoPoint(12.77, 77.45)));
        val data = "{\"point\":{\"lat\":12.77,\"lng\":77.45}}";
        val doc = Document.builder().data(mapper.readTree(data)).build();
        underTest.handle(doc);
        Assert.assertEquals(data, doc.getData().toString());
    }

    @Test
    public void testHandleWithMatchingFeatures() throws IOException, ProcessorInitializationException {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });

        val def = RegionMatchProcessorDefinition.builder().matchField("$.point").targetFieldRootMapping(ImmutableMap.of("$.featureName", "name")).build();
        val geoJsonReader = Mockito.mock(GeoJsonReader.class);
        val pointExtractor = mock(PointExtractor.class);
        val index = mock(ESRIGeoRegionIndex.class);
        val underTest = new RegionMatchProcessor(def, index, geoJsonReader, new PointExtractor(), false);
        when(geoJsonReader.fetchFeatures(anyString())).thenReturn(ImmutableList.of());
        underTest.init();
        when(pointExtractor.extractFromRoot(any(JsonNode.class))).thenReturn(Optional.of(new GeoPoint(12.77, 77.45)));
        val feature = new Feature();
        feature.setProperties(ImmutableMap.of("name", "test"));
        when(index.matchingIds(any())).thenReturn(ImmutableList.of(feature));
        val data = "{\"point\":{\"lat\":12.77,\"lng\":77.45}}";
        val doc = Document.builder().data(mapper.readTree(data)).build();
        underTest.handle(doc);
        val resultData = "{\"point\":{\"lat\":12.77,\"lng\":77.45},\"featureName\":\"test\"}";

        Assert.assertEquals(resultData, doc.getData().toString());

    }

    @Test
    public void testHandleWithMatchingFeaturesNestedTarget() throws IOException, ProcessorInitializationException {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });

        val def = RegionMatchProcessorDefinition.builder().matchField("$.point").targetFieldRootMapping(ImmutableMap.of("$.feature.featureName", "name")).build();
        val geoJsonReader = Mockito.mock(GeoJsonReader.class);
        val pointExtractor = mock(PointExtractor.class);
        val index = mock(ESRIGeoRegionIndex.class);
        val underTest = new RegionMatchProcessor(def, index, geoJsonReader, new PointExtractor(), false);
        when(geoJsonReader.fetchFeatures(anyString())).thenReturn(ImmutableList.of());
        underTest.init();
        when(pointExtractor.extractFromRoot(any(JsonNode.class))).thenReturn(Optional.of(new GeoPoint(12.77, 77.45)));
        val feature = new Feature();
        feature.setProperties(ImmutableMap.of("name", "test"));
        when(index.matchingIds(any())).thenReturn(ImmutableList.of(feature));
        val data = "{\"point\":{\"lat\":12.77,\"lng\":77.45}}";
        val doc = Document.builder().data(mapper.readTree(data)).build();
        underTest.handle(doc);
        val resultData = "{\"point\":{\"lat\":12.77,\"lng\":77.45},\"feature\":{\"featureName\":\"test\"}}";

        Assert.assertEquals(resultData, doc.getData().toString());
    }
}