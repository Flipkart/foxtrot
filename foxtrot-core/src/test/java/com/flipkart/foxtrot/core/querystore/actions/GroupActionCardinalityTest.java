package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.CardinalityOverflowException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/***
 Created by nitish.goyal on 24/07/18
 ***/

@Slf4j
public class GroupActionCardinalityTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getGroupDocumentsForEstimation(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .admin()
                .indices()
                .prepareRefresh("*")
                .execute()
                .actionGet();
        getTableMetadataManager().updateEstimationData(TestUtils.TEST_TABLE_NAME, 1397658117000L);
    }

    @Test
    public void testEstimationWithMultipleNestingHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Lists.newArrayList("os", "deviceId"));

        try {
            GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
            Assert.assertTrue(response.getResult()
                                      .containsKey("android"));
            Assert.assertTrue(response.getResult()
                                      .containsKey("ios"));
        } catch (CardinalityOverflowException e) {
            //Cardinality is over the allowed cardinality
        }
    }

    @Test
    public void testEstimationWithMultipleNesting() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
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
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("registered"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        Assert.assertTrue(response.getResult()
                                  .containsKey("0"));

    }

    @Test
    public void testEstimationPercentileCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("value"));
        try {
            GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
            Assert.assertTrue(response.getResult()
                                      .containsKey("0"));
        } catch (CardinalityOverflowException e) {
            //Cardinality is greater than allowed
        }

    }
}
