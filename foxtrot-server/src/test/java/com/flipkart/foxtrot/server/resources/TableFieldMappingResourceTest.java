/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMappingResourceTest extends FoxtrotResourceTest {

    private final FoxtrotTableManager tableManager;

    @Rule
    public ResourceTestRule resources;

    public TableFieldMappingResourceTest() throws Exception {
        super();
        tableManager = mock(FoxtrotTableManager.class);
        when(tableManager.getAll()).thenReturn(Collections.singletonList(TestUtils.TEST_TABLE));
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new TableFieldMappingResource(tableManager, getTableMetadataManager()))
                .build();
    }

    @Ignore
    @Test
    public void testGet() throws Exception {
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments(getMapper()));
        await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

        Set<FieldMetadata> mappings = new HashSet<>();
        mappings.add(FieldMetadata.builder()
                .field("time")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("word")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("data.data")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("header.hello")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("head.hello")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.dayOfWeek")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.minuteOfDay")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.monthOfYear")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.hourOfDay")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.year")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.dayOfMonth")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("date.minuteOfHour")
                .type(FieldType.LONG)
                .build());

        TableFieldMapping tableFieldMapping = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, mappings);
        String response = resources
                .target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .request()
                .get(String.class);

        TableFieldMapping mapping = getMapper().readValue(response, TableFieldMapping.class);
        assertEquals(tableFieldMapping.getTable(), mapping.getTable());
        assertTrue(tableFieldMapping.getMappings()
                .equals(mapping.getMappings()));
    }

    @Test
    public void testGetInvalidTable() throws Exception {
        try {
            resources
                    .target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME + "-missing"))
                    .request()
                    .head();
        } catch (WebApplicationException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse()
                    .getStatus());
        }
    }

    @Test
    public void testGetTableWithNoDocument() throws Exception {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<>());
        TableFieldMapping response = resources
                .target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .request()
                .get(TableFieldMapping.class);
        assertEquals(request.getTable(), response.getTable());
        assertNotNull(response.getMappings());
    }

    @Test
    public void getAllFields() throws Exception {
        doNothing().when(getQueryStore())
                .initializeTable(any(String.class));

        getQueryStore().save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments(getMapper()));
        await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

        Map<String, TableFieldMapping> response = resources
                .target("/v1/tables/fields")
                .request()
                .get(new GenericType<Map<String, TableFieldMapping>>() {
                });
        //System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response));
        Assert.assertFalse(response.isEmpty());
        Assert.assertEquals(1, response.size());
        Assert.assertTrue(response.containsKey(TestUtils.TEST_TABLE_NAME));
    }

}
