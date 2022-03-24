/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.exception.BadRequestException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitorAdapter;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.datetime.TimeWindow;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import org.joda.time.Interval;

import java.util.List;

public class PeriodSelector extends FilterVisitorAdapter<Void> {

    private final TimeWindow timeWindow = new TimeWindow();
    private final List<Filter> filters;

    public PeriodSelector(final List<Filter> filters) {
        super(null);
        this.filters = filters;
        timeWindow.setStartTime(Long.MIN_VALUE);
        timeWindow.setEndTime(Long.MAX_VALUE);
    }

    public Interval analyze() {
        return analyze(System.currentTimeMillis());
    }

    public Interval analyze(long currentTime) {
        for (Filter filter : filters) {
            if (filter.isFilterTemporal()) {
                filter.accept(this);
            }
        }
        for (Filter filter : filters) {
            if (ElasticsearchUtils.TIME_FIELD.equals(filter.getField())) {
                filter.accept(this);
            }
        }

        if (timeWindow.getStartTime() > timeWindow.getEndTime()) {
            throw new BadRequestException("Invalid temporal filters",
                    new IllegalArgumentException("Invalid temporal filters in query"));
        }

        timeWindow.setStartTime(timeWindow.getStartTime() == Long.MIN_VALUE
                ? 0
                : timeWindow.getStartTime());
        timeWindow.setEndTime(timeWindow.getEndTime() == Long.MAX_VALUE
                ? currentTime
                : timeWindow.getEndTime());
        return new Interval(timeWindow.getStartTime(), timeWindow.getEndTime());
    }

    @Override
    public Void visit(BetweenFilter betweenFilter) {
        timeWindow.setStartTime(Math.max((Long) betweenFilter.getFrom(), timeWindow.getStartTime()));
        timeWindow.setEndTime(Math.min((Long) betweenFilter.getTo(), timeWindow.getEndTime()));
        return null;
    }

    @Override
    public Void visit(GreaterThanFilter greaterThanFilter) {
        timeWindow.setStartTime(Math.max((Long) greaterThanFilter.getValue() + 1, timeWindow.getStartTime()));
        return null;
    }

    @Override
    public Void visit(GreaterEqualFilter greaterEqualFilter) {
        timeWindow.setStartTime(Math.max((Long) greaterEqualFilter.getValue(), timeWindow.getStartTime()));
        return null;
    }

    @Override
    public Void visit(LessThanFilter lessThanFilter) {
        timeWindow.setEndTime(Math.min((Long) lessThanFilter.getValue() - 1, timeWindow.getEndTime()));
        return null;
    }

    @Override
    public Void visit(LessEqualFilter lessEqualFilter) {
        timeWindow.setEndTime(Math.min((Long) lessEqualFilter.getValue(), timeWindow.getEndTime()));
        return null;
    }

    @Override
    public Void visit(LastFilter lastFilter) {
        TimeWindow window = lastFilter.getWindow();
        timeWindow.setStartTime(Math.max(window.getStartTime(), timeWindow.getStartTime()));
        timeWindow.setEndTime(Math.min(window.getEndTime(), timeWindow.getEndTime()));
        return null;
    }

}
