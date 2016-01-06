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
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMappingResourceTest extends FoxtrotResourceTest {

    public TableFieldMappingResourceTest() throws Exception {
        super();
        doReturn(true).when(getTableMetadataManager()).exists(TestUtils.TEST_TABLE_NAME);
        doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(anyString());
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new TableFieldMappingResource(getQueryStore()));
        addProvider(FoxtrotExceptionMapper.class);
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
        String response = client().resource(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .get(String.class);

        TableFieldMapping mapping = getMapper().readValue(response, TableFieldMapping.class);
        assertEquals(tableFieldMapping.getTable(), mapping.getTable());
        assertTrue(tableFieldMapping.getMappings().equals(mapping.getMappings()));
    }

    @Test
    public void testGetInvalidTable() throws Exception {
        ClientResponse clientResponse = client().resource(
                String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME + "-missing")).head();
        assertEquals(ClientResponse.Status.NOT_FOUND, clientResponse.getClientResponseStatus());
    }

    @Test
    public void testGetTableWithNoDocument() throws Exception {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<>());
        TableFieldMapping response = client().resource(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME))
                .get(TableFieldMapping.class);

        assertEquals(request.getTable(), response.getTable());
        assertNull(response.getMappings());
    }
}
