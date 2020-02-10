/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.query;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.MissingFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotInFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import java.util.List;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:31 PM
 */
public class ElasticSearchQueryGenerator implements FilterVisitor<Void> {
    private final BoolQueryBuilder boolFilterBuilder;

    public ElasticSearchQueryGenerator() {
        this.boolFilterBuilder = boolQuery();
    }

    @Override
    public Void visit(BetweenFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).from(filter.getFrom())
                          .to(filter.getTo()));
        return null;
    }

    @Override
    public Void visit(EqualsFilter filter) {
        addFilter(termQuery(Utils.storedFieldName(filter.getField()), filter.getValue()));
        return null;
    }

    @Override
    public Void visit(NotEqualsFilter filter) {
        addFilter(boolQuery().mustNot(termQuery(Utils.storedFieldName(filter.getField()), filter.getValue())));
        return null;
    }

    @Override
    public Void visit(ContainsFilter filter) {
        addFilter(queryStringQuery(filter.getValue()).defaultField(filter.getField()));
        return null;
    }

    @Override
    public Void visit(GreaterThanFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).gt(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(GreaterEqualFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).gte(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(LessThanFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).lt(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(LessEqualFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).lte(filter.getValue()));
        return null;
    }

    @Override
    public Void visit(AnyFilter filter) {
        addFilter(matchAllQuery());
        return null;
    }

    @Override
    public Void visit(InFilter filter) {
        addFilter(termsQuery(Utils.storedFieldName(filter.getField()), filter.getValues()));
        return null;
    }

    @Override
    public Void visit(NotInFilter notInFilter) {
        addFilter(boolQuery().mustNot(
                termsQuery(Utils.storedFieldName(notInFilter.getField()), notInFilter.getValues())));
        return null;
    }

    public Void visit(ExistsFilter filter) {
        addFilter(existsQuery(Utils.storedFieldName(filter.getField())));
        return null;
    }

    @Override
    public Void visit(LastFilter filter) {
        addFilter(rangeQuery(Utils.storedFieldName(filter.getField())).from(filter.getWindow()
                                                                                    .getStartTime())
                          .to(filter.getWindow()
                                      .getEndTime()));
        return null;
    }

    @Override
    public Void visit(MissingFilter filter) {
        addFilter(boolQuery().mustNot(existsQuery(Utils.storedFieldName(filter.getField()))));
        return null;
    }

    @Override
    public Void visit(WildCardFilter filter) {
        addFilter(wildcardQuery(Utils.storedFieldName(filter.getField()), filter.getValue()
                .concat("*")));
        return null;
    }

    private void addFilter(QueryBuilder queryBuilder) {
        boolFilterBuilder.filter(queryBuilder);
    }

    public QueryBuilder genFilter(List<Filter> filters) {
        for (Filter filter : filters) {
            filter.accept(this);
        }
        return QueryBuilders.constantScoreQuery(boolFilterBuilder);
    }

}
