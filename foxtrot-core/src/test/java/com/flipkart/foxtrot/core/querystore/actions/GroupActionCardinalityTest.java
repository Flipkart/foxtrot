package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.CardinalityOverflowException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/***
 Created by nitish.goyal on 24/07/18
 ***/

@Slf4j
public class GroupActionCardinalityTest extends ActionTest {

    private static final String CARDINALITY_TEST_TABLE = "cardinality-test-table";
    private static final Long time = DateTime.now()
            .minusDays(1)
            .toDate()
            .getTime();

    @Before
    public void setUp() throws Exception {
        super.setup();
        tableMetadataManager.save(Table.builder()
                .name(CARDINALITY_TEST_TABLE)
                .ttl(30)
                .build());
        List<Document> documents = TestUtils.getGroupDocumentsForEstimation(getMapper(), time);
        getQueryStore().save(CARDINALITY_TEST_TABLE, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        getTableMetadataManager().getFieldMappings(CARDINALITY_TEST_TABLE, true, true, time);
        ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig()
                .setMaxCardinality(15000);
        getTableMetadataManager().updateEstimationData(CARDINALITY_TEST_TABLE, time);
    }

    @After
    public void afterMethod() {
        ElasticsearchTestUtils.cleanupIndex(getElasticsearchConnection(),
                ElasticsearchUtils.getCurrentIndex(CARDINALITY_TEST_TABLE, time));
    }

    @Test(expected = CardinalityOverflowException.class)
    public void testEstimationWithMultipleNestingHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Lists.newArrayList("os", "deviceId"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        Assert.assertTrue(response.getResult()
                .containsKey("android"));
        Assert.assertTrue(response.getResult()
                .containsKey("ios"));
    }

    @Test
    public void testEstimationWithMultipleNesting() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Lists.newArrayList("os", "registered"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));

        Assert.assertTrue(response.getResult()
                .containsKey("android"));
        Assert.assertTrue(response.getResult()
                .containsKey("ios"));
    }

    @Test
    public void testEstimationBooleanCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("registered"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        Assert.assertTrue(response.getResult()
                .containsKey("0"));
    }

    @Test
    public void testEstimationPercentileCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("value"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        Assert.assertTrue(response.getResult()
                .containsKey("0"));
    }
}
