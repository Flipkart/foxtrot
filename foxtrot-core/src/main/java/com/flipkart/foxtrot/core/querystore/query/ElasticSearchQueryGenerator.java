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
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:31 PM
 */
public class ElasticSearchQueryGenerator extends FilterVisitor {
    private final BoolQueryBuilder boolFilterBuilder;
    private final FilterCombinerType combinerType;

    public ElasticSearchQueryGenerator(FilterCombinerType combinerType) {
        this.boolFilterBuilder = boolQuery();
        this.combinerType = combinerType;
    }

    @Override
    public void visit(BetweenFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).from(filter.getFrom()).to(filter.getTo()));
    }

    @Override
    public void visit(EqualsFilter filter) throws Exception {
        addFilter(termQuery(filter.getField(), filter.getValue()));
    }

    @Override
    public void visit(NotEqualsFilter filter) throws Exception {
        addFilter(boolQuery().mustNot(termQuery(filter.getField(), filter.getValue())));
    }

    @Override
    public void visit(ContainsFilter filter) throws Exception {
        addFilter(queryStringQuery(filter.getValue()).defaultField(String.format("%s.analyzed", filter.getField())));
    }

    @Override
    public void visit(GreaterThanFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).gt(filter.getValue()));
    }

    @Override
    public void visit(GreaterEqualFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).gte(filter.getValue()));
    }

    @Override
    public void visit(LessThanFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).lt(filter.getValue()));
    }

    @Override
    public void visit(LessEqualFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).lte(filter.getValue()));
    }

    @Override
    public void visit(AnyFilter filter) throws Exception {
        addFilter(matchAllQuery());
    }

    @Override
    public void visit(InFilter filter) throws Exception {
        addFilter(termsQuery(filter.getField(), filter.getValues()));
    }

    @Override
    public void visit(NotInFilter notInFilter) throws Exception {
        addFilter(boolQuery().mustNot(termsQuery(notInFilter.getField(), notInFilter.getValues())));
    }

    public void visit(ExistsFilter filter) throws Exception {
        addFilter(existsQuery(filter.getField()));
    }

    @Override
    public void visit(LastFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField())
                .from(filter.getWindow().getStartTime())
                .to(filter.getWindow().getEndTime()));
    }

    @Override
    public void visit(MissingFilter filter) throws Exception {
        addFilter(boolQuery().mustNot(existsQuery(filter.getField())));
    }

    private void addFilter(QueryBuilder queryBuilder) throws Exception {
        if (combinerType == FilterCombinerType.and) {
            boolFilterBuilder.filter(queryBuilder);
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
