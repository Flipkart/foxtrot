package com.flipkart.foxtrot.core.querystore.handlers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.util.MetricUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class MetricRecorderTest {

    private MetricRecorder metricRecorder;
    private Method registerFilterUsageMetrics;
    private MetricRegistry mockMetricRegistry;

    @Before
    public void setup() throws NoSuchMethodException {
        metricRecorder = new MetricRecorder();
        registerFilterUsageMetrics = MetricRecorder.class.getDeclaredMethod("registerFilterUsageMetrics",
                ActionRequest.class);
        registerFilterUsageMetrics.setAccessible(true);

        SerDe.init(new ObjectMapper());
        mockMetricRegistry = Mockito.mock(MetricRegistry.class);
        Mockito.when(mockMetricRegistry.meter(Mockito.anyString()))
                .thenReturn(new Meter());
        MetricUtil.setup(mockMetricRegistry);

    }

    @Test
    public void testFilterMetrics() throws InvocationTargetException, IllegalAccessException {

        List<Filter> filters = new ArrayList<>();

        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("eventData.browser");
        containsFilter.setValue("rome");

        EqualsFilter equalsFilter = EqualsFilter.builder()
                .field("os")
                .value("android")
                .build();

        filters.add(containsFilter);
        filters.add(equalsFilter);

        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable("test-table");
        groupRequest.setFilters(filters);

        registerFilterUsageMetrics.invoke(metricRecorder, groupRequest);
        Mockito.verify(mockMetricRegistry, Mockito.atLeast(1))
                .meter("com.flipkart.foxtrot.core.filter.contains");
        Mockito.verify(mockMetricRegistry, Mockito.atLeast(1))
                .meter("com.flipkart.foxtrot.core.filter.equals");
        Mockito.verify(mockMetricRegistry, Mockito.atLeast(1))
                .meter("com.flipkart.foxtrot.core.filter.test-table.contains");
        Mockito.verify(mockMetricRegistry, Mockito.atLeast(1))
                .meter("com.flipkart.foxtrot.core.filter.test-table.equals");
    }


}
