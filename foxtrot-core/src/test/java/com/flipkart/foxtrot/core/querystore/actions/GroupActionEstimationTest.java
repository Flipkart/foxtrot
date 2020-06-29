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
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.joda.time.DateTime;

/**
 * Tests cardinality estimation
 */
@Slf4j
public class GroupActionEstimationTest extends ActionTest {

    private static final String PROBABILITY_ESTIMATION_TABLE = "probability-test-table";
    private static final Long time = DateTime.now()
            .minusDays(1)
            .toDate()
            .getTime();

    @Before
    public void setUp() throws Exception {
        super.setup();
        tableMetadataManager.save(Table.builder()
                .name(PROBABILITY_ESTIMATION_TABLE)
                .ttl(30)
                .build());

        List<Document> documents = TestUtils.getGroupDocumentsForEstimation(getMapper(), time);
        getQueryStore().save(PROBABILITY_ESTIMATION_TABLE, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        getTableMetadataManager().getFieldMappings(PROBABILITY_ESTIMATION_TABLE, true, true, time);
        ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig()
                .setMaxCardinality(MAX_CARDINALITY);
        getTableMetadataManager().updateEstimationData(PROBABILITY_ESTIMATION_TABLE, time);
    }

    @After
    public void afterMethod() {
        ElasticsearchTestUtils.cleanupIndex(getElasticsearchConnection(),
                ElasticsearchUtils.getCurrentIndex(PROBABILITY_ESTIMATION_TABLE, time));
    }

    @Test
    public void testEstimationNoFilter() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
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
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));

        getQueryExecutor().execute(groupRequest);
    }

    @Test
    // High cardinality field queries are allowed if in a small time span
    public void testEstimationTemporalFilterHighCardinality() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
        groupRequest.setNesting(Collections.singletonList("deviceId"));

        long time = DateTime.now()
                .minusDays(1)
                .toDate()
                .getTime();

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
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
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
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
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
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
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
        groupRequest.setTable(PROBABILITY_ESTIMATION_TABLE);
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
