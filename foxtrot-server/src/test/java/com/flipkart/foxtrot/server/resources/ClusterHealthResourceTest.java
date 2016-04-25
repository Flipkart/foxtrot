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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by swapnil on 25/01/16.
 */
public class ClusterHealthResourceTest extends FoxtrotResourceTest{

    public ClusterHealthResourceTest() throws Exception {
        super();
        Mockito.doReturn(true).when(getTableMetadataManager()).exists(Mockito.anyString());
        Mockito.doReturn(TestUtils.TEST_TABLE).when(getTableMetadataManager()).get(Mockito.anyString());
    }

    @Override
    protected void setUpResources() throws Exception {
        addResource(new ClusterHealthResource(getQueryStore()));
    }

    @Test
    public void testClusterHealthApi(){
        ClusterHealthResponse response = client().resource("/v1/clusterhealth").get(ClusterHealthResponse.class);
        Assert.assertEquals("elasticsearch", response.getClusterName());
        Assert.assertEquals(1,response.getNumberOfNodes());
        Assert.assertNotNull(response.getIndices().get("table-meta"));
    }

    @Test
    public void testNodeStats() throws FoxtrotException {

        JsonNode response = client().resource("/v1/clusterhealth/nodestats").get(JsonNode.class);
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

        JsonNode response = client().resource("/v1/clusterhealth/indicesstats").get(JsonNode.class);
        Assert.assertEquals(2, response.path("primaries").path("docs").path("count").asInt());
        Assert.assertNotEquals(0, response.path("total").path("store").path("sizeInBytes").asInt());
        Assert.assertNotEquals(0, response.path("primaries").path("store").path("sizeInBytes").asInt());
    }
}
