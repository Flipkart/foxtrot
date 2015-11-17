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
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class TableFieldMappingResourceTest {
    private static QueryStore queryStore = mock(QueryStore.class);

    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder().addResource(new TableFieldMappingResource(queryStore)).build();

    @Before
    public void setUp() throws Exception {
        reset(queryStore);
    }

    @Test
    public void testGet() throws Exception {
        Set<FieldTypeMapping> mappings = new HashSet<FieldTypeMapping>();
        mappings.add(new FieldTypeMapping("word", FieldType.STRING));
        mappings.add(new FieldTypeMapping("data.data", FieldType.STRING));
        mappings.add(new FieldTypeMapping("header.hello", FieldType.STRING));
        mappings.add(new FieldTypeMapping("head.hello", FieldType.LONG));

        TableFieldMapping tableFieldMapping = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, mappings);
        doReturn(tableFieldMapping).when(queryStore).getFieldMappings(anyString());
        Response response = resource.client().target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME)).request().get();

        TableFieldMapping mapping = response.readEntity(TableFieldMapping.class);
        assertEquals(tableFieldMapping.getTable(), mapping.getTable());
        assertTrue(tableFieldMapping.getMappings().equals(mapping.getMappings()));
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetInvalidTable() throws Exception {
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE, "Dummy")).when(queryStore).getFieldMappings(anyString());
        Response response = resource.client().target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME + "-missing")).request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void testGetTableInternalError() throws Exception {
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.METADATA_FETCH_ERROR, "Dummy")).when(queryStore).getFieldMappings(anyString());
        Response response = resource.client().target(String.format("/v1/tables/%s/fields", TestUtils.TEST_TABLE_NAME)).request().get();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }
}
