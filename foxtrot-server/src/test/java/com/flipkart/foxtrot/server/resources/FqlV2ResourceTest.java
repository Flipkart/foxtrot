package com.flipkart.foxtrot.server.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.core.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.gandalf.access.AccessServiceImpl;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlGetRequest;
import com.flipkart.foxtrot.sql.fqlstore.FqlStore;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import io.dropwizard.testing.junit.ResourceTestRule;
import java.util.List;
import javax.ws.rs.client.Entity;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class FqlV2ResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new FqlV2Resource(
                    new FqlEngine(getTableMetadataManager(), getQueryStore(), getQueryExecutorFactory(), getMapper()),
                    new FqlStoreServiceImpl(getElasticsearchConnection(), getMapper()),
                    new AccessServiceImpl(new FoxtrotServerConfiguration(), new TableActionRequestVisitor()),
                    new QueryConfig()))
            .addProvider(new FoxtrotExceptionMapper(getMapper()))
            .setMapper(objectMapper)
            .build();

    @Test
    public void test0() {
        String query = "show tables";
        Entity<String> stringEntity = Entity.json(query);
        resources.client()
                .target("/v2/fql")
                .request()
                .post(stringEntity);
    }

    @Test
    public void test() {
        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle("testQuery");
        fqlStore.setQuery("select * from test");
        Entity<FqlStore> fqlStoreEntity = Entity.json(fqlStore);
        resources.client()
                .target("/v2/fql/save")
                .request()
                .post(fqlStoreEntity);
    }

    @Test
    public void test1() throws InterruptedException {
        String title = "title1";
        String query = "show tables";

        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle(title);
        fqlStore.setQuery(query);
        Entity<FqlStore> fqlStoreEntity = Entity.json(fqlStore);
        FqlStore fqlStore1 = resources.client()
                .target("/v2/fql/save")
                .request()
                .post(fqlStoreEntity, FqlStore.class);
        Assert.assertNotNull(fqlStore1);
        Assert.assertEquals(title, fqlStore1.getTitle());
        Assert.assertEquals(query, fqlStore1.getQuery());

        Thread.sleep(1000);

        FqlGetRequest fqlGetRequest = new FqlGetRequest();
        fqlGetRequest.setTitle(title);
        Entity<FqlGetRequest> fqlGetRequestEntity = Entity.json(fqlGetRequest);
        List<FqlStore> result = getMapper().convertValue(resources.client()
                .target("/v2/fql/get")
                .request()
                .post(fqlGetRequestEntity, List.class), new TypeReference<List<FqlStore>>() {
        });
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(title, result.get(0)
                .getTitle());
        Assert.assertEquals(query, result.get(0)
                .getQuery());
    }
}
