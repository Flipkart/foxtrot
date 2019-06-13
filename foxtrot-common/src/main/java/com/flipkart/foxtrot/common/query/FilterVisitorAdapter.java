package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;

/**
 * Created by santanu on 6/7/17.
 */
public class FilterVisitorAdapter<T> implements FilterVisitor<T> {

    private final T defaultValue;

    protected FilterVisitorAdapter(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public T visit(BetweenFilter betweenFilter) {
        return defaultValue;
    }

    @Override
    public T visit(EqualsFilter equalsFilter) {
        return defaultValue;
    }

    @Override
    public T visit(NotEqualsFilter notEqualsFilter) {
        return defaultValue;
    }

    @Override
    public T visit(ContainsFilter stringContainsFilterElement) {
        return defaultValue;
    }

    @Override
    public T visit(GreaterThanFilter greaterThanFilter) {
        return defaultValue;
    }

    @Override
    public T visit(GreaterEqualFilter greaterEqualFilter) {
        return defaultValue;
    }

    @Override
    public T visit(LessThanFilter lessThanFilter) {
        return defaultValue;
    }

    @Override
    public T visit(LessEqualFilter lessEqualFilter) {
        return defaultValue;
    }

    @Override
    public T visit(AnyFilter anyFilter) {
        return defaultValue;
    }

    @Override
    public T visit(InFilter inFilter) {
        return defaultValue;
    }

    @Override
    public T visit(NotInFilter notInFilter) {
        return defaultValue;
    }

    @Override
    public T visit(ExistsFilter existsFilter) {
        return defaultValue;
    }

    @Override
    public T visit(LastFilter lastFilter) {
        return defaultValue;
    }

    @Override
    public T visit(MissingFilter missingFilter) {
        return defaultValue;
    }

    @Override
    public T visit(WildCardFilter wildCardFilter) {
        return defaultValue;
    }
}
