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
public class ElasticSearchQueryGenerator extends FilterVisitor<Void> {
    private final BoolQueryBuilder boolFilterBuilder;

    public ElasticSearchQueryGenerator() {
        this.boolFilterBuilder = boolQuery();
    }

    @Override
    public Void visit(BetweenFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).from(filter.getFrom()).to(filter.getTo()));
        return null;
    }

    @Override
    public Void visit(EqualsFilter filter) throws Exception {
        addFilter(termQuery(filter.getField(), filter.getValue()));
        return null;
    }

    @Override
    public Void visit(NotEqualsFilter filter) throws Exception {
        addFilter(boolQuery().mustNot(termQuery(filter.getField(), filter.getValue())));
        return null;
    }

    @Override
    public Void visit(ContainsFilter filter) throws Exception {
        addFilter(queryStringQuery(filter.getValue()).defaultField(String.format("%s.analyzed", filter.getField())));
        return null;
    }

    @Override
    public Void visit(GreaterThanFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).gt(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(GreaterEqualFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).gte(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(LessThanFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).lt(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(LessEqualFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField()).lte(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(AnyFilter filter) throws Exception {
        addFilter(matchAllQuery());
        return null;
    }

    @Override
    public Void visit(InFilter filter) throws Exception {
        addFilter(termsQuery(filter.getField(), filter.getValues()));
        return null;
    }

    @Override
    public Void visit(NotInFilter notInFilter) throws Exception {
        addFilter(boolQuery().mustNot(termsQuery(notInFilter.getField(), notInFilter.getValues())));
        return null;
    }

    public Void visit(ExistsFilter filter) throws Exception {
        addFilter(existsQuery(filter.getField()));
        return null;
    }

    @Override
    public Void visit(LastFilter filter) throws Exception {
        addFilter(rangeQuery(filter.getField())
                .from(filter.getWindow().getStartTime())
                .to(filter.getWindow().getEndTime()));
        return null;
    }

    @Override
    public Void visit(MissingFilter filter) throws Exception {
        addFilter(boolQuery().mustNot(existsQuery(filter.getField())));
        return null;
    }

    private void addFilter(QueryBuilder queryBuilder) {
        boolFilterBuilder.filter(queryBuilder);
    }

    public QueryBuilder genFilter(List<Filter> filters) throws Exception {
        for (Filter filter : filters) {
            filter.accept(this);
        }
        return QueryBuilders.constantScoreQuery(boolFilterBuilder);
    }

}
