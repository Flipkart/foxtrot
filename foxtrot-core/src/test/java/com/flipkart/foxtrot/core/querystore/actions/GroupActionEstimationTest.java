package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.CardinalityOverflowException;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests cardinality estimation
 */
@Slf4j
public class GroupActionEstimationTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getGroupDocumentsForEstimation(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").execute().actionGet();
        getTableMetadataManager().updateEstimationData(TestUtils.TEST_TABLE_NAME, 1397658117000L);
    }

    @Test
    public void testEstimationNoFilter() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("os"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));

        Assert.assertTrue(response.getResult().containsKey("android"));
        Assert.assertTrue(response.getResult().containsKey("ios"));
    }


   /* @Test(expected = CardinalityOverflowException.class)
    // Block queries on high cardinality fields
    public void testEstimationNoFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));

        getQueryExecutor().execute(groupRequest);
    }*/

    @Test
    // High cardinality field queries are allowed if in a small timespan
    public void testEstimationTemporalFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(
                BetweenFilter.builder()
                        .field("_timestamp")
                        .temporal(true)
                        .from(1397658117000L)
                        .to(1397658117000L + 2 * 60000)
                        .build()
        ));

        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response));
        Assert.assertTrue(response.getResult().isEmpty());
    }


    @Test
    // High cardinality field queries are allowed if scoped in small cardinality field
    public void testEstimationCardinalFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(
                EqualsFilter.builder()
                        .field("os")
                        .value("ios")
                        .build()
        ));

        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response));
        Assert.assertFalse(response.getResult().isEmpty());
    }

    /*@Test(expected = CardinalityOverflowException.class)
    // High cardinality field queries not are allowed
    public void testEstimationGTFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(
                GreaterThanFilter.builder()
                        .field("value")
                        .value(10)
                        .build()
        ));
        getQueryExecutor().execute(groupRequest);
    }*/

    @Test
    // High cardinality field queries with filters including small subset are allowed
    public void testEstimationLTFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(
                LessThanFilter.builder()
                        .field("value")
                        .value(30)
                        .build()
        ));
        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response));
        Assert.assertFalse(response.getResult().isEmpty());
    }

    /*@Test(expected = CardinalityOverflowException.class)
    // High cardinality field queries with filters including small subset are allowed
    public void testEstimationLTFilterHighCardinalityBlocked() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(
                LessThanFilter.builder()
                        .field("value")
                        .value(80)
                        .build()
        ));
        log.debug(getMapper().writerWithDefaultPrettyPrinter().writeValueAsString(groupRequest));
        getQueryExecutor().execute(groupRequest);
    }*/

}
