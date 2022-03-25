package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.exception.BadRequestException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.datetime.RoundingMode;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.LessEqualFilter;
import io.dropwizard.util.Duration;
import org.assertj.core.util.Lists;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class PeriodSelectorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @Test
    public void shouldTakeIntersectionOfLastAndGreaterEqualFilters() {

        LastFilter lastFilter = new LastFilter("_timestamp", 1615287737666L, Duration.minutes(1000), RoundingMode.NONE);
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter("_timestamp", 1601384825501L, true);

        List<Filter> filters = Lists.newArrayList(lastFilter, greaterEqualFilter);

        PeriodSelector periodSelector = new PeriodSelector(filters);
        Interval interval = periodSelector.analyze();
        Assert.assertEquals(lastFilter.getWindow()
                .getStartTime(), interval.getStartMillis());
        Assert.assertEquals(lastFilter.getWindow()
                .getEndTime(), interval.getEndMillis());

    }

    @Test
    public void shouldThrowExceptionForNoIntersectingWindows() {
        exception.expect(BadRequestException.class);
        BetweenFilter betweenFilter1 = new BetweenFilter("_timestamp", 1615271400000L, 1615272000000L, true);
        BetweenFilter betweenFilter2 = new BetweenFilter("_timestamp", 1615307400000L, 1615308000000L, true);

        List<Filter> filters = Lists.newArrayList(betweenFilter1, betweenFilter2);

        PeriodSelector periodSelector = new PeriodSelector(filters);
        Interval interval = periodSelector.analyze();
        Assert.assertEquals(0, interval.getStartMillis());
        Assert.assertTrue(System.currentTimeMillis() - interval.getEndMillis() < 1000);

    }


    @Test
    public void shouldTakeIntersectionOfGreaterEqualAndLessEqualFilter() {

        LessEqualFilter lessEqualFilter = new LessEqualFilter("_timestamp", 1615287737666L, true);
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter("_timestamp", 1601384825501L, true);

        List<Filter> filters = Lists.newArrayList(lessEqualFilter, greaterEqualFilter);

        PeriodSelector periodSelector = new PeriodSelector(filters);
        Interval interval = periodSelector.analyze();
        Assert.assertEquals(greaterEqualFilter.getValue(), interval.getStartMillis());
        Assert.assertEquals(lessEqualFilter.getValue(), interval.getEndMillis());

    }

}
