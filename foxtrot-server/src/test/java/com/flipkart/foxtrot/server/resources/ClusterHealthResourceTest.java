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

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

/**
 * Created by swapnil on 25/01/16.
 */
public class ClusterHealthResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    public ClusterHealthResourceTest() throws Exception {
        super();
        doReturn(true).when(getTableMetadataManager()).exists(anyString());
        doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(anyString());
        resources = ResourceTestRule.builder()
                .addResource(new ClusterHealthResource(getQueryStore()))
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .setMapper(getMapper())
                .build();
    }

    @Test
    public void testClusterHealthApi() {
        JsonNode response = resources.client().target("/v1/clusterhealth").request().get(JsonNode.class);
        Assert.assertEquals(1, response.get("numberOfNodes").asInt());
        Assert.assertNotNull(response.get("indices").has("table-meta"));
    }

    @Test
    public void testNodeStats() throws FoxtrotException {
        JsonNode response = resources.client().target("/v1/clusterhealth/nodestats").request().get(JsonNode.class);
        Assert.assertEquals(1, response.path("nodesMap").size());
    }

    @Test
    public void testIndicesStats() throws FoxtrotException {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), getMapper().getNodeFactory().objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), getMapper().getNodeFactory().objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        JsonNode response = resources.client().target("/v1/clusterhealth/indicesstats").request().get(JsonNode.class);
        Assert.assertEquals(2, response.path("primaries").path("docs").path("count").asInt());
        Assert.assertNotEquals(0, response.path("total").path("store").path("sizeInBytes").asInt());
        Assert.assertNotEquals(0, response.path("primaries").path("store").path("sizeInBytes").asInt());
    }
}
