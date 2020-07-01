package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.CardinalityOverflowException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.ImmutableList;
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
        List<Document> documents = TestUtils.getTestDocumentsForCardinalityEstimation(getMapper(), time, 300);
        getQueryStore().save(CARDINALITY_TEST_TABLE, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        getTableMetadataManager().getFieldMappings(CARDINALITY_TEST_TABLE, true, true, time);
        ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig()
                .setMaxCardinality(1500);
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
    public void testEstimationPercentileCardinality() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("value"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        Assert.assertFalse(response.getResult().isEmpty());
    }

    @Test
    public void testEstimationNoFilter() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("os"));

        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));

        Assert.assertTrue(response.getResult()
                .containsKey("android"));
        Assert.assertTrue(response.getResult()
                .containsKey("ios"));
    }

    // Block queries on high cardinality fields
    @Test(expected = CardinalityOverflowException.class)
    public void testEstimationNoFilterHighCardinality() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));

        getQueryExecutor().execute(groupRequest);
    }

    @Test
    // High cardinality field queries are allowed if in a small time span
    public void testEstimationTemporalFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));

        groupRequest.setFilters(ImmutableList.of(BetweenFilter.builder()
                .field("_timestamp")
                .temporal(true)
                .from(time)
                .to(time + 2 * 60000)
                .build()));

        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(response));
        Assert.assertFalse(response.getResult()
                .isEmpty());
    }


    @Test
    // High cardinality field queries are allowed if scoped in small cardinality field
    public void testEstimationCardinalFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(EqualsFilter.builder()
                .field("os")
                .value("ios")
                .build()));

        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(response));
        Assert.assertFalse(response.getResult()
                .isEmpty());
    }

    @Test(expected = CardinalityOverflowException.class)
    public void testEstimationGTFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(GreaterThanFilter.builder()
                .field("value")
                .value(10)
                .build()));
        getQueryExecutor().execute(groupRequest);
    }

    @Test
    // High cardinality field queries with filters including small subset are allowed
    public void testEstimationLTFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(LessThanFilter.builder()
                .field("value")
                .value(3)
                .build()));
        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(groupRequest));
        GroupResponse response = GroupResponse.class.cast(getQueryExecutor().execute(groupRequest));
        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(response));
        Assert.assertFalse(response.getResult()
                .isEmpty());
    }

    @Test(expected = CardinalityOverflowException.class)
    public void testEstimationLTFilterHighCardinalityBlocked() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(CARDINALITY_TEST_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));
        groupRequest.setFilters(ImmutableList.of(LessThanFilter.builder()
                .field("value")
                .value(80)
                .build()));
        log.debug(getMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(groupRequest));
        getQueryExecutor().execute(groupRequest);
    }
}
