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
package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class HistogramActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getHistogramDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchServer().getClient().admin().indices().prepareRefresh("*").setForce(true).execute().actionGet();
    }


    @Test(expected = FoxtrotException.class)
    public void testHistogramActionAnyException() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        doReturn(null).when(getElasticsearchConnection()).getClient();
        getQueryExecutor().execute(histogramRequest);
    }

    @Test
    public void testHistogramActionIntervalMinuteNoFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397651100000L, 2));
        counts.add(new HistogramResponse.Count(1397658060000L, 3));
        counts.add(new HistogramResponse.Count(1397658180000L, 1));
        counts.add(new HistogramResponse.Count(1397758200000L, 1));
        counts.add(new HistogramResponse.Count(1397958060000L, 1));
        counts.add(new HistogramResponse.Count(1398653100000L, 2));
        counts.add(new HistogramResponse.Count(1398658200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalMinuteWithFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.minutes);
        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));
        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397651100000L, 1));
        counts.add(new HistogramResponse.Count(1397658060000L, 2));
        counts.add(new HistogramResponse.Count(1397658180000L, 1));
        counts.add(new HistogramResponse.Count(1397958060000L, 1));
        counts.add(new HistogramResponse.Count(1398658200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalHourNoFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.hours);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));

        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397649600000L, 2));
        counts.add(new HistogramResponse.Count(1397656800000L, 4));
        counts.add(new HistogramResponse.Count(1397757600000L, 1));
        counts.add(new HistogramResponse.Count(1397955600000L, 1));
        counts.add(new HistogramResponse.Count(1398650400000L, 2));
        counts.add(new HistogramResponse.Count(1398657600000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalHourWithFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.hours);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));


        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397649600000L, 1));
        counts.add(new HistogramResponse.Count(1397656800000L, 3));
        counts.add(new HistogramResponse.Count(1397955600000L, 1));
        counts.add(new HistogramResponse.Count(1398657600000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalDayNoFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.days);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397606400000L, 6));
        counts.add(new HistogramResponse.Count(1397692800000L, 1));
        counts.add(new HistogramResponse.Count(1397952000000L, 1));
        counts.add(new HistogramResponse.Count(1398643200000L, 3));
        assertTrue(response.getCounts().equals(counts));
    }

    @Test
    public void testHistogramActionIntervalDayWithFilter() throws FoxtrotException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE_NAME);
        histogramRequest.setPeriod(Period.days);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        histogramRequest.setFilters(Lists.<Filter>newArrayList(greaterThanFilter, lessThanFilter));

        HistogramResponse response = HistogramResponse.class.cast(getQueryExecutor().execute(histogramRequest));
        List<HistogramResponse.Count> counts = new ArrayList<HistogramResponse.Count>();
        counts.add(new HistogramResponse.Count(1397606400000L, 4));
        counts.add(new HistogramResponse.Count(1397952000000L, 1));
        counts.add(new HistogramResponse.Count(1398643200000L, 1));
        assertTrue(response.getCounts().equals(counts));
    }
}
