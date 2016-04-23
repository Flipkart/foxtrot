/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMappingResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    public TableFieldMappingResourceTest() throws Exception {
        super();
        doReturn(true).when(getTableMetadataManager()).exists(TestUtils.TEST_TABLE_NAME);
        doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(anyString());
        resources = ResourceTestRule.builder()
                .addResource(new TableFieldMappingResource(getQueryStore()))
                .setMapper(getMapper())
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .build();
    }

    @Test
    public void testGet() throws Exception {
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments(getMapper()));
        Thread.sleep(500);

        Set<FieldTypeMapping> mappings = new HashSet<FieldTypeMapping>();
        mappings.add(new FieldTypeMapping("word", FieldType.STRING));
        mappings.add(new FieldTypeMapping("data.data", FieldType.STRING));
        mappings.add(new FieldTypeMapping("header.hello", FieldType.STRING));
        mappings.add(new FieldTypeMapping("head.hello", FieldType.LONG));

        TableFieldMapping tableFieldMapping = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, mappings);
        String response = resources.client().target(String.format("/foxtrot/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .request()
                .get(String.class);

        TableFieldMapping mapping = getMapper().readValue(response, TableFieldMapping.class);
        assertEquals(tableFieldMapping.getTable(), mapping.getTable());
        assertTrue(tableFieldMapping.getMappings().equals(mapping.getMappings()));
    }

    @Test
    public void testGetInvalidTable() throws Exception {
        try {
            resources.client().target(
                    String.format("/foxtrot/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME + "-missing")).request().head();
        } catch(WebApplicationException ex) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getResponse().getStatus());
        }
    }

    @Test
    public void testGetTableWithNoDocument() throws Exception {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<>());
        TableFieldMapping response = resources.client().target(String.format("/foxtrot/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .request()
                .get(TableFieldMapping.class);
        assertEquals(request.getTable(), response.getTable());
        assertNull(response.getMappings());
    }
}
