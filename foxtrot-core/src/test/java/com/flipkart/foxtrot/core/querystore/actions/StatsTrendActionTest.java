/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.actions;

import static com.flipkart.foxtrot.core.TestUtils.TEST_EMAIL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class StatsTrendActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getStatsTrendDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .admin()
                .indices()
                .prepareRefresh("*")
                .execute()
                .actionGet();
    }

    @Test
    public void testStatsTrendActionWithoutNesting() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(5, statsTrendResponse.getResult()
                .size());
        assertEquals(8, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertEquals(7, statsTrendResponse.getResult()
                .get(0)
                .getPercentiles()
                .size());
        assertNull(statsTrendResponse.getBuckets());
    }

    private void filterNonZeroCounts(StatsTrendResponse statsTrendResponse) {
        statsTrendResponse.getResult()
                .removeIf(statsTrendValue -> statsTrendValue.getStats()
                        .containsKey("count") && statsTrendValue.getStats()
                        .get("count")
                        .equals(0L));
    }

    @Test
    public void testStatsTrendActionNoExtendedStat() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(EnumSet.allOf(Stat.class)
                .stream()
                .filter(x -> !x.isExtended())
                .collect(Collectors.toSet()));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(5, statsTrendResponse.getResult()
                .size());
        assertEquals(5, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertEquals(7, statsTrendResponse.getResult()
                .get(0)
                .getPercentiles()
                .size());
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlyCountStat() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.COUNT));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .containsKey("count"));
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlyMaxStat() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.MAX));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .containsKey("max"));
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlyMinStat() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.MIN));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .containsKey("min"));
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlyAvgStat() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.AVG));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .containsKey("avg"));
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlySumStat() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setStats(Collections.singleton(Stat.SUM));
        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getStats()
                .containsKey("sum"));
        assertNull(statsTrendResponse.getBuckets());
    }

    @Test
    public void testStatsTrendActionOnlyOnePercentile() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setPercentiles(Collections.singletonList(5d));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        filterNonZeroCounts(statsTrendResponse);
        assertNotNull(statsTrendResponse);
        assertNotNull(statsTrendResponse.getResult());
        assertEquals(1, statsTrendResponse.getResult()
                .get(0)
                .getPercentiles()
                .size());
        assertTrue(statsTrendResponse.getResult()
                .get(0)
                .getPercentiles()
                .containsKey(5d));
    }

    @Test
    public void testStatsTrendActionWithNesting() throws FoxtrotException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os"));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        assertNotNull(statsTrendResponse);
        assertNull(statsTrendResponse.getResult());
        assertNotNull(statsTrendResponse.getBuckets());
        assertEquals(3, statsTrendResponse.getBuckets()
                .size());
    }

    @Test
    public void testStatsTrendActionWithMultiLevelNesting() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setFrom(1L);
        betweenFilter.setTo(System.currentTimeMillis());
        betweenFilter.setTemporal(true);
        betweenFilter.setField("_timestamp");
        request.setFilters(Collections.<Filter>singletonList(betweenFilter));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        assertNotNull(statsTrendResponse);
        assertNull(statsTrendResponse.getResult());
        assertNotNull(statsTrendResponse.getBuckets());
        assertEquals(3, statsTrendResponse.getBuckets()
                .size());

        assertEquals(1, statsTrendResponse.getBuckets()
                .get(0)
                .getBuckets()
                .size());
        assertEquals(2, statsTrendResponse.getBuckets()
                .get(1)
                .getBuckets()
                .size());
        assertEquals(1, statsTrendResponse.getBuckets()
                .get(2)
                .getBuckets()
                .size());
    }

    @Test
    public void testStatsTrendActionWithNoFilter() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));

        StatsTrendResponse statsTrendResponse = StatsTrendResponse.class.cast(
                getQueryExecutor().execute(request, TEST_EMAIL));
        assertNull(statsTrendResponse);
    }

    @Test(expected = MalformedQueryException.class)
    public void testStatsTrendActionNullTable() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(null);
        request.setTimestamp("_timestamp");
        request.setField("battery");
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request, TEST_EMAIL);
    }

    @Test(expected = MalformedQueryException.class)
    public void testStatsTrendActionNullField() throws FoxtrotException, JsonProcessingException {
        StatsTrendRequest request = new StatsTrendRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setTimestamp("_timestamp");
        request.setField(null);
        request.setNesting(Lists.newArrayList("os", "version"));
        getQueryExecutor().execute(request, TEST_EMAIL);
    }
}
