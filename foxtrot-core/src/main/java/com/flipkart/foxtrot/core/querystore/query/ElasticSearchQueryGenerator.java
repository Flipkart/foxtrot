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
package com.flipkart.foxtrot.core.querystore.query;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.datetime.TimeWindow;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import org.elasticsearch.index.query.*;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:31 PM
 */
public class ElasticSearchQueryGenerator extends FilterVisitor {
    private final BoolFilterBuilder boolFilterBuilder;
    private final FilterCombinerType combinerType;

    public ElasticSearchQueryGenerator(FilterCombinerType combinerType) {
        this.boolFilterBuilder = FilterBuilders.boolFilter();
        this.combinerType = combinerType;
    }

    @Override
    public void visit(BetweenFilter betweenFilter) throws Exception {
        addFilter(FilterBuilders.rangeFilter(betweenFilter.getField())
                .from(betweenFilter.getFrom())
                .to(betweenFilter.getTo())
                .cache(!betweenFilter.isTemporal()));
    }

    @Override
    public void visit(EqualsFilter equalsFilter) throws Exception {
        addFilter(FilterBuilders.termFilter(equalsFilter.getField(), equalsFilter.getValue()));
    }

    @Override
    public void visit(NotEqualsFilter notEqualsFilter) throws Exception {
        addFilter(FilterBuilders.boolFilter().mustNot(
                FilterBuilders.termFilter(notEqualsFilter.getField(), notEqualsFilter.getValue())));
    }

    @Override
    public void visit(ContainsFilter stringContainsFilterElement) throws Exception {
        addFilter(
                FilterBuilders.queryFilter(
                        QueryBuilders.queryString(
                                stringContainsFilterElement.getValue())
                                .defaultField(stringContainsFilterElement.getField() + ".analyzed"))
                        .cache(false));
    }

    @Override
    public void visit(GreaterThanFilter greaterThanFilter) throws Exception {
        addFilter(
                FilterBuilders.rangeFilter(greaterThanFilter.getField())
                        .gt(greaterThanFilter.getValue())
                        .cache(!greaterThanFilter.isTemporal()));
    }

    @Override
    public void visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
        addFilter(
                FilterBuilders.rangeFilter(greaterEqualFilter.getField())
                        .gte(greaterEqualFilter.getValue())
                        .cache(!greaterEqualFilter.isTemporal()));
    }

    @Override
    public void visit(LessThanFilter lessThanFilter) throws Exception {
        addFilter(
                FilterBuilders.rangeFilter(lessThanFilter.getField())
                        .lt(lessThanFilter.getValue())
                        .cache(!lessThanFilter.isTemporal()));

    }

    @Override
    public void visit(LessEqualFilter lessEqualFilter) throws Exception {
        addFilter(
                FilterBuilders.rangeFilter(lessEqualFilter.getField())
                        .lte(lessEqualFilter.getValue())
                        .cache(!lessEqualFilter.isTemporal()));

    }

    @Override
    public void visit(AnyFilter anyFilter) throws Exception {
        addFilter(FilterBuilders.matchAllFilter());
    }

    @Override
    public void visit(InFilter inFilter) throws Exception {
        addFilter(
                FilterBuilders.inFilter(inFilter.getField(), inFilter.getValues()));
    }

    @Override
    public void visit(ExistsFilter existsFilter) throws Exception {
        addFilter(FilterBuilders.existsFilter(existsFilter.getField()));
    }

    @Override
    public void visit(LastFilter lastFilter) throws Exception {
        TimeWindow timeWindow = lastFilter.getWindow();
        addFilter(
                FilterBuilders.rangeFilter(lastFilter.getField())
                        .from(timeWindow.getStartTime())
                        .to(timeWindow.getEndTime())
                        .cache(false));
    }

    @Override
    public void visit(MissingFilter missingFilter) throws Exception {
        addFilter(FilterBuilders.missingFilter(missingFilter.getField()));
    }

    private void addFilter(FilterBuilder elasticSearchFilter) throws Exception {
        if (combinerType == FilterCombinerType.and) {
            boolFilterBuilder.must(elasticSearchFilter);
            return;
        }
        //boolFilterBuilder.should(elasticSearchFilter);
        throw new UnsupportedOperationException(String.format("%s is not supported", FilterCombinerType.or.name()));
    }

    public QueryBuilder genFilter(List<Filter> filters) throws Exception {
        for (Filter filter : filters) {
            filter.accept(this);
        }
        return QueryBuilders.constantScoreQuery(boolFilterBuilder);
    }

}
