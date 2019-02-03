package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;

/**
 * Created by santanu on 6/7/17.
 */
public class FilterVisitorAdapter<T> extends FilterVisitor<T> {

    final T defaultValue;

    protected FilterVisitorAdapter(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public T visit(BetweenFilter betweenFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(EqualsFilter equalsFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(NotEqualsFilter notEqualsFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(ContainsFilter stringContainsFilterElement) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(GreaterThanFilter greaterThanFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(LessThanFilter lessThanFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(LessEqualFilter lessEqualFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(AnyFilter anyFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(InFilter inFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(NotInFilter notInFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(ExistsFilter existsFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(LastFilter lastFilter) throws Exception {
        return defaultValue;
    }

    @Override
    public T visit(MissingFilter missingFilter) throws Exception {
        return defaultValue;
    }
}
