package com.flipkart.foxtrot.core.querystore.query;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
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
    //private  andFilterBuilder;
    //private OrFilterBuilder orFilterBuilder;
    private BoolQueryBuilder queryBuilder;
    private FilterCombinerType combinerType;

    public ElasticSearchQueryGenerator(FilterCombinerType combinerType) {
        this.queryBuilder = QueryBuilders.boolQuery();
        this.combinerType = combinerType;
    }

    @Override
    public void visit(BetweenFilter betweenFilter) throws Exception {
        addFilter(QueryBuilders.rangeQuery(betweenFilter.getField())
                .from(betweenFilter.getFrom())
                .to(betweenFilter.getTo()));
    }

    @Override
    public void visit(EqualsFilter equalsFilter) throws Exception {
        addFilter(QueryBuilders.termQuery(equalsFilter.getField(), equalsFilter.getValue()));
    }

    @Override
    public void visit(NotEqualsFilter notEqualsFilter) throws Exception {
        addFilter(QueryBuilders.boolQuery().mustNot(
                QueryBuilders.termQuery(notEqualsFilter.getField(), notEqualsFilter.getValue())));
    }

    @Override
    public void visit(ContainsFilter stringContainsFilterElement) throws Exception {
        addFilter(
                QueryBuilders.regexpQuery(stringContainsFilterElement.getField(),
                        stringContainsFilterElement.getExpression()));
    }

    @Override
    public void visit(GreaterThanFilter greaterThanFilter) throws Exception {
        addFilter(
                QueryBuilders.rangeQuery(greaterThanFilter.getField())
                        .gt(greaterThanFilter.getValue()));
    }

    @Override
    public void visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
        addFilter(
                QueryBuilders.rangeQuery(greaterEqualFilter.getField())
                                        .gte(greaterEqualFilter.getValue()));
    }

    @Override
    public void visit(LessThanFilter lessThanFilter) throws Exception {
        addFilter(
                QueryBuilders.rangeQuery(lessThanFilter.getField())
                                     .lt(lessThanFilter.getValue()));

    }

    @Override
    public void visit(LessEqualFilter lessEqualFilter) throws Exception {
        addFilter(
                QueryBuilders.rangeQuery(lessEqualFilter.getField())
                                    .lte(lessEqualFilter.getValue()));

    }

    @Override
    public void visit(AnyFilter anyFilter) throws Exception {
        addFilter(QueryBuilders.matchAllQuery());
    }

    private void addFilter(QueryBuilder query) throws Exception {
        if(combinerType == FilterCombinerType.and) {
            queryBuilder.must(query);
        }
        queryBuilder.should(query);
    }

    public BoolQueryBuilder genFilter(List<Filter> filters) throws Exception {
        if(null == filters || filters.isEmpty()) {
            addFilter(QueryBuilders.matchAllQuery());
        }
        else {
            for(Filter filter : filters) {
                filter.accept(this);
            }
        }
        return queryBuilder;//andFilterBuilder != null ? andFilterBuilder : orFilterBuilder;
    }

}
