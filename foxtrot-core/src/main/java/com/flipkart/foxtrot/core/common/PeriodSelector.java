/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.datetime.TimeWindow;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import org.joda.time.Interval;

import java.util.List;

public class PeriodSelector extends FilterVisitor {

    private final TimeWindow timeWindow = new TimeWindow();
    private final List<Filter> filters;

    public PeriodSelector(final List<Filter> filters) {
        this.filters = filters;
        timeWindow.setStartTime(Long.MAX_VALUE);
        timeWindow.setEndTime(Long.MIN_VALUE);
    }

    public Interval analyze() throws Exception {
        return analyze(System.currentTimeMillis());
    }

    public Interval analyze(long currentTime) throws Exception {
        for (Filter filter : filters) {
            if (filter.isTemporal()) {
                filter.accept(this);
            }
        }
        timeWindow.setStartTime(timeWindow.getStartTime() == Long.MAX_VALUE ? 0 : timeWindow.getStartTime());
        timeWindow.setEndTime(timeWindow.getEndTime() == Long.MIN_VALUE ? currentTime : timeWindow.getEndTime());
        return new Interval(timeWindow.getStartTime(), timeWindow.getEndTime());
    }

    @Override
    public void visit(BetweenFilter betweenFilter) throws Exception {
        timeWindow.setStartTime(Math.min((Long) betweenFilter.getFrom(), timeWindow.getStartTime()));
        timeWindow.setEndTime(Math.max((Long) betweenFilter.getTo(), timeWindow.getEndTime()));
    }

    @Override
    public void visit(EqualsFilter equalsFilter) throws Exception {
    }

    @Override
    public void visit(NotEqualsFilter notEqualsFilter) throws Exception {
    }

    @Override
    public void visit(ContainsFilter stringContainsFilterElement) throws Exception {
    }

    @Override
    public void visit(GreaterThanFilter greaterThanFilter) throws Exception {
        timeWindow.setStartTime(Math.min((Long) greaterThanFilter.getValue() + 1, timeWindow.getStartTime()));
    }

    @Override
    public void visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
        timeWindow.setStartTime(Math.min((Long) greaterEqualFilter.getValue(), timeWindow.getStartTime()));
    }

    @Override
    public void visit(LessThanFilter lessThanFilter) throws Exception {
        timeWindow.setEndTime(Math.max((Long) lessThanFilter.getValue() - 1, timeWindow.getEndTime()));
    }

    @Override
    public void visit(LessEqualFilter lessEqualFilter) throws Exception {
        timeWindow.setEndTime(Math.max((Long) lessEqualFilter.getValue(), timeWindow.getEndTime()));
    }

    @Override
    public void visit(AnyFilter anyFilter) throws Exception {

    }

    @Override
    public void visit(InFilter inFilter) throws Exception {

    }

    @Override
    public void visit(ExistsFilter existsFilter) throws Exception {

    }

    @Override
    public void visit(LastFilter lastFilter) throws Exception {
        TimeWindow window = lastFilter.getWindow();
        timeWindow.setStartTime(window.getStartTime());
        timeWindow.setEndTime(window.getEndTime());
    }

    @Override
    public void visit(MissingFilter missingFilter) throws Exception {

    }
}
