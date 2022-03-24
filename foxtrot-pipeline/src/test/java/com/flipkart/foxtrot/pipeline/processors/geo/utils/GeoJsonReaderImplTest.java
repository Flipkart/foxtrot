package com.flipkart.foxtrot.pipeline.processors.geo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.testing.junit.DropwizardClientRule;
import lombok.val;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GeoJsonReaderImplTest {

    @ClassRule
    public static final DropwizardClientRule dropwizard = new DropwizardClientRule(
            new GeoJsonReaderImplTest.PipelineResource());

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoJsonReadUnknownScheme() throws IOException {
        val mapper = new ObjectMapper();
        val underTest = new GeoJsonReaderImpl(mapper);
        underTest.fetchFeatures("https://geojson.io/text");
    }

    @Test
    public void testGeoJsonReadResScheme() throws IOException {
        val mapper = new ObjectMapper();
        val underTest = new GeoJsonReaderImpl(mapper);
        val features = underTest.fetchFeatures("res://testgeojson.json");
        assertEquals(2, features.size());
    }

    @Test
    public void testGeoJsonReadResSchemeWithFilter() throws IOException {
        val mapper = new ObjectMapper();
        val underTest = new GeoJsonReaderImpl(mapper);
        val features = underTest.fetchFeatures("res://testgeojson.json", ImmutableSet.of("1"));
        assertEquals(1, features.size());
    }

    @Test
    public void testGeoJsonReadHttpScheme() throws IOException {
        val host = dropwizard.baseUri()
                .getHost();
        val port = dropwizard.baseUri()
                .getPort();
        val basePath = dropwizard.baseUri()
                .getPath();
        val mapper = new ObjectMapper();
        val underTest = new GeoJsonReaderImpl(mapper);
        val features = underTest.fetchFeatures(String.format("http://%s:%s%s/v1/geojson", host, port, basePath));
        assertEquals(1, features.size());
    }

    @Path("/v1/geojson")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static class PipelineResource {

        @GET
        @Path("/")
        public Response ping() {
            return Response.ok("{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\","
                    + "\"properties\": {},\"geometry\": {\"type\": \"Polygon\",\"coordinates\": "
                    + "[[[87.21221923828125,25.249664387120884],[86.58050537109375,24.963650200508965],"
                    + "[87.41546630859375,24.614560364814544],[87.802734375,25.582085278700696],"
                    + "[87.21221923828125,25.249664387120884]]]}}]}")
                    .build();

        }
    }
}