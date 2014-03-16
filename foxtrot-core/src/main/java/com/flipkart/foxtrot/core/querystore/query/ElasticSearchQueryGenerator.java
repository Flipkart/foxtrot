package com.flipkart.foxtrot.core.querystore.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.root.AndCombinerFilter;
import com.flipkart.foxtrot.common.query.root.OrCombinerFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.google.common.collect.Lists;
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
    private BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    @Override
    public void visit(BetweenFilter betweenFilter) throws Exception {
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(betweenFilter.getField())
                                                                .from(betweenFilter.getFrom())
                                                                .to(betweenFilter.getTo());
        this.queryBuilder.must(queryBuilder);
        //addFilter(rangeFilterBuilder);
    }

    @Override
    public void visit(EqualsFilter equalsFilter) throws Exception {
        this.queryBuilder.must(
                                QueryBuilders.termQuery(equalsFilter.getField(), equalsFilter.getValue()));
    }

    @Override
    public void visit(NotEqualsFilter notEqualsFilter) throws Exception {
        //addFilter(FilterBuilders.notFilter(
        //        FilterBuilders.termFilter(notEqualsFilter.getField(), notEqualsFilter.getValue())));
        this.queryBuilder.mustNot(
                QueryBuilders.termQuery(notEqualsFilter.getField(), notEqualsFilter.getValue()));
    }

    @Override
    public void visit(ContainsFilter stringContainsFilterElement) throws Exception {
        this.queryBuilder.must(
                QueryBuilders.regexpQuery(stringContainsFilterElement.getField(),
                        stringContainsFilterElement.getExpression()));
    }

    @Override
    public void visit(AndCombinerFilter andCombinerFilter) throws Exception {
        //
        //andFilterBuilder = FilterBuilders.andFilter();
        for(Filter filter : andCombinerFilter.getFilters()) {
            filter.accept(this);
        }
    }

    @Override
    public void visit(OrCombinerFilter orCombinerFilter) throws Exception {
        //orFilterBuilder = FilterBuilders.orFilter(); TODO:: USE SHOULD
        for(Filter filter : orCombinerFilter.getFilters()) {
            filter.accept(this);
        }
    }

    @Override
    public void visit(GreaterThanFilter greaterThanFilter) throws Exception {
        this.queryBuilder.must(
                QueryBuilders.rangeQuery(greaterThanFilter.getField())
                        .gt(greaterThanFilter.getValue()));
    }

    @Override
    public void visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
        this.queryBuilder.must(
                QueryBuilders.rangeQuery(greaterEqualFilter.getField())
                                        .gte(greaterEqualFilter.getValue()));
    }

    @Override
    public void visit(LessThanFilter lessThanFilter) throws Exception {
        this.queryBuilder.must(
                QueryBuilders.rangeQuery(lessThanFilter.getField())
                                     .lt(lessThanFilter.getValue()));

    }

    @Override
    public void visit(LessEqualFilter lessEqualFilter) throws Exception {
        this.queryBuilder.must(
                QueryBuilders.rangeQuery(lessEqualFilter.getField())
                                    .lte(lessEqualFilter.getValue()));

    }

    /*private void addFilter(FilterBuilder filterBuilder) throws Exception {
        if(null != andFilterBuilder) {
            andFilterBuilder.add(filterBuilder);
        } else {
            if(null != orFilterBuilder) {
                orFilterBuilder.add(filterBuilder);
            }
            else {
                //TODO::Throw exception
            }
        }
    }*/

    public QueryBuilder genFilter(Filter filter) throws Exception {
        filter.accept(this);
        return queryBuilder;//andFilterBuilder != null ? andFilterBuilder : orFilterBuilder;
    }

    public static void main(String[] args) throws Exception {
        OrCombinerFilter orCombinerFilter = new OrCombinerFilter();
        List<Filter> filters = Lists.newArrayList();
        {
            GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
            greaterEqualFilter.setField("path");
            greaterEqualFilter.setValue(3.9);
            filters.add(greaterEqualFilter);
        }
        {
            LessThanFilter lessThanFilter = new LessThanFilter();
            lessThanFilter.setField("path2");
            lessThanFilter.setValue(2);
            filters.add(lessThanFilter);
        }
        orCombinerFilter.setFilters(filters);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        System.out.println(mapper.writeValueAsString(orCombinerFilter));
        ElasticSearchQueryGenerator queryGenerator = new ElasticSearchQueryGenerator();
        System.out.println(queryGenerator.genFilter(orCombinerFilter));
    }
}
